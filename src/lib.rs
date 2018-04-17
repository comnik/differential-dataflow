//! Differential dataflow is a high-throughput, low-latency data-parallel programming framework.
//!
//! Differential dataflow programs are written in a collection-oriented style, where you transform
//! collections of records using traditional operations like `map`, `filter`, `join`, and `group_by`.
//! Differential dataflow also includes the less traditional operation `iterate`, which allows you
//! to repeatedly apply differential dataflow transformations to collections.
//!
//! Once you have defined a differential dataflow computation, you may then add records to or remove
//! records from its inputs; the system will automatically update the computation's outputs with the
//! appropriate corresponding additions and removals, and report these changes to you.
//!
//! Differential dataflow is built on the [timely dataflow](https://github.com/frankmcsherry/timely-dataflow)
//! framework for data-parallel programming which automatically parallelizes across multiple threads,
//! processes, and computers. Furthermore, because it uses timely dataflow's primitives, it seamlessly
//! inter-operates with other timely dataflow computations.
//!
//! Differential dataflow is still very much a work in progress, with features and ergonomics still
//! wildly in development. It is generally improving, though.
//!
//! # Examples
//!
//! This fragment creates a collection of pairs of integers, imagined as graph edges, and then counts
//! first the number of times the source coordinate occurs, and then the number of times each count
//! occurs, giving us a sense for the distribution of degrees in the graph.
//!
//! ```ignore
//! // create a a degree counting differential dataflow
//! let (mut input, probe) = worker.dataflow(|scope| {
//!
//!     // create edge input, count a few ways.
//!     let (input, edges) = scope.new_collection();
//!
//!     // extract the source field, and then count.
//!     let degrs = edges.map(|(src, _dst)| src)
//!                      .count();
//!
//!     // extract the count field, and then count them.
//!     let distr = degrs.map(|(_src, cnt)| cnt)
//!                      .count();
//!
//!     // report the changes to the count collection, notice when done.
//!     let probe = distr.inspect(|x| println!("observed: {:?}", x))
//!                      .probe();
//!
//!     (input, probe)
//! });
//! ```
//!
//! Now assembled, we can drive the computation like a timely dataflow computation, by pushing update
//! records (triples of data, time, and change in count) at the `input` stream handle. The `probe` is
//! how timely dataflow tells us that we have seen all corresponding output updates (in case there are
//! none).
//!
//! ```ignore
//! loop {
//!     let time = input.epoch();
//!     for round in time .. time + 100 {
//!         input.advance_to(round);
//!         input.insert((round % 13, round % 7));
//!     }
//!
//!     input.flush();
//!     while probe.less_than(input.time()) {
//!        worker.step();
//!     }
//! }
//! ```
//!
//! This example should print out the 100 changes in the output, in this case each reflecting the increase
//! of some node degree by one (typically four output changes, corresponding to the addition and deletion
//! of the new and old counts of the old and new degrees of the affected node).

// #![forbid(missing_docs)]
#![feature(proc_macro)]

use std::fmt::Debug;

pub use collection::{Collection, AsCollection};
pub use hashable::Hashable;
pub use difference::Diff;

/// A composite trait for data types usable in differential dataflow.
///
/// Most differential dataflow operators require the ability to cancel corresponding updates, and the
/// way that they do this is by putting the data in a canonical form. The `Ord` trait allows us to sort
/// the data, at which point we can consolidate updates for equivalent records.
pub trait Data : timely::ExchangeData + Ord + Debug { }
impl<T: timely::ExchangeData + Ord + Debug> Data for T { }

extern crate fnv;
extern crate timely;
extern crate timely_sort;
extern crate timely_communication;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

pub mod hashable;
pub mod operators;
pub mod algorithms;
pub mod lattice;
pub mod trace;
pub mod input;
pub mod difference;
pub mod collection;

//
// JS INTEROP
//

#[macro_use] extern crate stdweb;
#[macro_use] extern crate serde_derive;

use stdweb::js_export;

use std::rc::Rc;
// use std::hash::Hash;
// use std::cmp::Ordering;
// use lattice::Lattice;

use timely::{Root, Allocator};
// use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::{Handle, Probe};
// use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::execute::{setup_threadless};

use input::{Input, InputSession};
use trace::{BatchReader, Cursor};
use trace::implementations::ord::{OrdKeyBatch, OrdValBatch};
use trace::implementations::spine::Spine;
use operators::arrange::{ArrangeByKey, ArrangeBySelf, TraceAgent};
use operators::join::JoinCore;

//
// TYPES
//

type Entity = u64;
type Attribute = u32;

#[derive(Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub enum Value {
    Eid(Entity),
    Attribute(Attribute),
    Number(i64),
}

js_serializable!(Value);
js_deserializable!(Value);

// #[derive(Eq, Clone, Serialize, Deserialize, Abomonation, Debug)]
#[derive(Serialize, Deserialize)]
pub struct JsDatom {
    e: Entity,
    a: Attribute,
    v: Value,
}

type Datom = (Entity, Attribute, Value);

js_serializable!(JsDatom);
js_deserializable!(JsDatom);

// type Scope<'a> = Child<'a, Root<Allocator>, usize>;
type RootTime = Product<RootTimestamp, usize>;
type ProbeHandle = Handle<RootTime>;
type InputHandle = InputSession<usize, Datom, isize>;
// type TraceBatch = OrdValBatch<Value, Value, RootTime, isize>;
// type TraceSpine = Spine<Value, Value, RootTime, isize, Rc<TraceBatch>>;
// type TraceHandle = TraceAgent<Value, Value, RootTime, isize, TraceSpine>;

type KeyIndex<K> = TraceAgent<K, (), RootTime, isize,
                              Spine<K, (), RootTime, isize,
                                    Rc<OrdKeyBatch<K, RootTime, isize>>>>;

type Index<K, V> = TraceAgent<K, V, RootTime, isize,
                              Spine<K, V, RootTime, isize,
                                    Rc<OrdValBatch<K, V, RootTime, isize>>>>;

//
// CONTEXT
//

struct DB {
    e_av: Index<Entity, (Attribute, Value)>,
    a_ev: Index<Attribute, (Entity, Value)>,
    ea_v: Index<(Entity, Attribute), Value>,
    av_e: Index<(Attribute, Value), Entity>,
}

pub struct Context {
    root: Root<Allocator>,
    input_handle: InputHandle,
    db: DB,
    probe: Option<ProbeHandle>,
    // output_callbacks,
}

static mut CTX: Option<Context> = None;

#[js_export]
pub fn setup() {
    unsafe {
        let mut root = setup_threadless();
        
        let (input_handle, db) = root.dataflow::<usize,_,_>(|scope| {
            let (input_handle, datoms) = scope.new_collection::<>();
            let db = DB {
                e_av: datoms.map(|(e, a, v)| (e, (a, v))).arrange_by_key().trace,
                a_ev: datoms.map(|(e, a, v)| (a, (e, v))).arrange_by_key().trace,
                ea_v: datoms.map(|(e, a, v)| ((e, a), v)).arrange_by_key().trace,
                av_e: datoms.map(|(e, a, v)| ((a, v), e)).arrange_by_key().trace,
            };

            (input_handle, db)
        });
        
        let ctx = Context {
            root,
            db,
            input_handle,
            probe: None,
        };

        CTX = Some(ctx);
    }
}

#[js_export]
pub fn register(clauses: Vec<Clause>) -> bool {
    // @TODO take a string key to distinguish output callbacks
    
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                ctx.probe = Some(parse_graph(ctx, clauses));
                true
            }
        }
    }
}

//
// QUERY GRAMMAR
//

struct Placeholder;
type Var = u32;

#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum Clause {
    Lookup(Entity, Attribute, Var),
    Entity(Entity, Var, Var),
    HasAttr(Var, Attribute, Var),
    Filter(Var, Attribute, Value),
}

js_serializable!(Clause);
js_deserializable!(Clause);

//
// RELATIONS
//

trait Relation {
    fn symbols(&self) -> &Vec<Var>;
    fn tuples(&mut self) -> &mut KeyIndex<Vec<Value>>;
}

struct SimpleRelation {
    symbols: Vec<Var>,
    tuples: KeyIndex<Vec<Value>>,
}

impl Relation for SimpleRelation {
    fn symbols(&self) -> &Vec<Var> { &self.symbols }
    fn tuples(&mut self) -> &mut KeyIndex<Vec<Value>> { &mut self.tuples }
}

//
// INTERPRETER
// 

trait Interpretable<R> where R : Relation {
    fn interpret(&self, ctx: &mut Context) -> R;
}

impl Interpretable<SimpleRelation> for Clause {
    fn interpret(&self, ctx: &mut Context) -> SimpleRelation {
        let db = &mut ctx.db;
        
        match self {
            &Clause::Lookup(e, a, sym1) => {
                let (_ea_in, rel) = ctx.root.dataflow::<usize, _, _>(|scope| {
                    let ea_in = scope.new_collection_from(vec![(e, a)]).1.arrange_by_self();
                    let rel = db.ea_v
                        .import(scope)
                        .join_core(&ea_in, |_, &v, _| {
                            let mut vs: Vec<Value> = Vec::with_capacity(8);
                            vs.push(v);

                            Some(vs)
                        })
                        .arrange_by_self()
                        .trace;
                    
                    (ea_in.trace, rel)
                });
                
                SimpleRelation { symbols: vec![sym1], tuples: rel }
            },
            &Clause::Entity(e, sym1, sym2) => {
                let (_e_in, rel) = ctx.root.dataflow::<usize, _, _>(|scope| {
                    let e_in = scope.new_collection_from(vec![e]).1.arrange_by_self();
                    let rel = db.e_av
                        .import(scope)
                        .join_core(&e_in, |_, &(a, v), _| {
                            let mut vs: Vec<Value> = Vec::with_capacity(8);
                            vs.push(Value::Attribute(a));
                            vs.push(v);

                            Some(vs)
                        })
                        .arrange_by_self()
                        .trace;
                    
                    (e_in.trace, rel)
                });
                
                SimpleRelation { symbols: vec![sym1, sym2], tuples: rel }
            },
            &Clause::HasAttr(sym1, a, sym2) => {
                let (_a_in, rel) = ctx.root.dataflow::<usize, _, _>(|scope| {
                    let a_in = scope.new_collection_from(vec![a]).1.arrange_by_self();
                    let rel = db.a_ev
                        .import(scope)
                        .join_core(&a_in, |_, &(e, v), _| {
                            let mut vs: Vec<Value> = Vec::with_capacity(8);
                            vs.push(Value::Eid(e));
                            vs.push(v);
                            
                            Some(vs)
                        })
                        .arrange_by_self()
                        .trace;
                    
                    (a_in.trace, rel)
                });
                
                SimpleRelation { symbols: vec![sym1, sym2], tuples: rel }
            },
            &Clause::Filter(sym1, a, v) => {
                let (_av_in, rel) = ctx.root.dataflow::<usize, _, _>(|scope| {
                    let av_in = scope.new_collection_from(vec![(a, v)]).1.arrange_by_self();
                    let rel = db.av_e
                        .import(scope)
                        .join_core(&av_in, |_, &e, _| {
                            let mut vs: Vec<Value> = Vec::with_capacity(8);
                            vs.push(Value::Eid(e));
                            
                            Some(vs)
                        })
                        .arrange_by_self()
                        .trace;
                    
                    (av_in.trace, rel)
                });

                SimpleRelation { symbols: vec![sym1], tuples: rel }
            }
        }
    }
}

fn join<R: Relation> (ctx: &mut Context, mut rel1: R, syms: Vec<Var>, mut rel2: R) -> SimpleRelation {
    let rel = ctx.root.dataflow::<usize, _, _>(|scope| {
        let r1 = rel1.tuples()
            .import(scope)
            .as_collection(|k, _v| (vec![k[0]], k.clone()))
            .arrange_by_key();
        
        let r2 = rel2.tuples()
            .import(scope)
            .as_collection(|k, _v| (vec![k[0]], k.clone()))
            .arrange_by_key();

        r1
            .join_core(&r2, |_key, v1, v2| {
                // @TODO can haz array here?
                let mut vstar = Vec::with_capacity(v1.len() + v2.len());

                vstar.append(&mut (*v1).clone());
                vstar.append(&mut (*v2).clone());
                
                Some(vstar)
            })
            .arrange_by_self()
            .trace
    });
    
    SimpleRelation {
        symbols: syms,
        tuples: rel
    }
}

// fn find<R: Relation> (ctx: &mut Context, rel: R, syms: Vec<Var>) -> SimpleRelation {
//     let projection = ctx.root.dataflow::<usize, _, _>(|scope| {
//         let rel = rel.tuples().import(scope);

//         rel
//             .map(|vs| {
//                 Some((*e, vstar))
//             })
//             .arrange_by_self()
//             .trace
//     });
    
//     SimpleRelation {
//         symbols: syms,
//         tuples: projection
//     }    
// }

/// This takes a potentially JS-generated description of a dataflow
/// graph and turns it into a differential dataflow.
fn parse_graph(ctx: &mut Context, mut clauses: Vec<Clause>) -> ProbeHandle {

    // @TODO pass a single scope around
    
    // @TODO output arrangement depends on whatever join comes next,
    // so maybe not do the arrangement in the interpretation of the
    // clause, but in the interpretation of the unification. should be
    // ok, as long as it's all inside a single scope

    let r1 = Interpretable::interpret(&clauses.pop().unwrap(), ctx);
    let r2 = Interpretable::interpret(&clauses.pop().unwrap(), ctx);
    let mut r3 = join(ctx, r1, vec![0], r2);
    // let r4 = find(ctx, r33, vec![0, 1, 2]);

    let probe = ctx.root.dataflow::<usize, _, _>(|scope| {
        let r3 = r3.tuples().import(scope);
        
        r3
            .stream
            .inspect(|batch_wrapper| {
                let batch = &batch_wrapper.item;
                let mut batch_cursor = batch.cursor();

                while batch_cursor.key_valid(&batch) {
                    // let vs = batch_cursor.val(&batch).clone();
                    let vs = batch_cursor.key(&batch);
                    js! {
                        const tuple = @{vs};
                        __UGLY_DIFF_HOOK(tuple);
                    }

                    batch_cursor.step_key(&batch);
                }
            })
            .probe()
    });

    probe
}

// #[js_export]
// pub fn demo() {
//     let mut root = setup_threadless();
//     let probe = root.dataflow::<(),_,_>(|scope| {
//         let stream = (0 .. 9)
//             .to_stream(scope)
//             .map(|x| x + 1)
//             .sink(Pipeline, "example", |input| {
//                 while let Some((time, data)) = input.next() {
//                     for datum in data.iter() {
//                         js! {
//                             var datum = @{datum};
//                             __UGLY_DIFF_HOOK(datum);
//                         }
//                     }
//                 }
//             });
//     });

//     root.step();
// }

#[js_export]
pub fn send(tx: usize, d: Vec<JsDatom>) -> bool {
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                for js_datom in d {
                    ctx.input_handle.insert((js_datom.e, js_datom.a, js_datom.v));
                }
                ctx.input_handle.advance_to(tx + 1);
                ctx.input_handle.flush();

                match ctx.probe {
                    None => false,
                    Some(ref mut probe) => {
                        while probe.less_than(ctx.input_handle.time()) {
                            ctx.root.step();
                        }

                        true
                    }
                }
            }
        }
    }
}
