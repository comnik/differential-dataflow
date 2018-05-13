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

use std::string::String;
use std::rc::Rc;
use std::boxed::Box;
use std::ops::Deref;
// use std::hash::Hash;
// use std::cmp::Ordering;

use timely::{Allocator};
use timely::dataflow::Scope;
use timely::dataflow::scopes::{Root, Child};
// use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::{Handle};
// use timely::dataflow::channels::pact::Pipeline;
use timely::progress::Timestamp;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::execute::{setup_threadless};
use timely_communication::Allocate;

use lattice::Lattice;
use input::{Input, InputSession};
use trace::implementations::ord::{OrdValBatch};
use trace::implementations::spine::Spine;
use operators::arrange::{ArrangeByKey, ArrangeBySelf, TraceAgent};
use operators::group::Threshold;
use operators::join::{JoinCore};
use operators::iterate::Variable;

//
// TYPES
//

type Entity = u64;
type Attribute = u32;

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub enum Value {
    Eid(Entity),
    Attribute(Attribute),
    Number(i64),
    String(String),
}

js_serializable!(Value);
js_deserializable!(Value);

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
pub struct Datom(Entity, Attribute, Value);

// @TODO This might not even be needed anymore
js_serializable!(Datom);
js_deserializable!(Datom);

static DB_ADD: u8 = 0;
static DB_RETRACT: u8 = 1;

#[derive(Deserialize)]
pub struct TxData(u8, Entity, Attribute, Value);

js_deserializable!(TxData);

#[derive(Serialize)]
pub struct Out(Vec<Value>, isize);

js_serializable!(Out);

type ProbeHandle<T> = Handle<Product<RootTimestamp, T>>;
// type TraceBatch = OrdValBatch<Value, Value, Product<RootTimestamp, T>, isize>;
// type TraceSpine = Spine<Value, Value, Product<RootTimestamp, T>, isize, Rc<TraceBatch>>;
// type TraceHandle = TraceAgent<Value, Value, Product<RootTimestamp, T>, isize, TraceSpine>;

type Index<K, V, T> = TraceAgent<K, V, Product<RootTimestamp, T>, isize,
                                 Spine<K, V, Product<RootTimestamp, T>, isize,
                                       Rc<OrdValBatch<K, V, Product<RootTimestamp, T>, isize>>>>;

//
// CONTEXT
//

struct DB<T: Timestamp+Lattice> {
    e_av: Index<Entity, (Attribute, Value), T>,
    a_ev: Index<Attribute, (Entity, Value), T>,
    ea_v: Index<(Entity, Attribute), Value, T>,
    av_e: Index<(Attribute, Value), Entity, T>,
}

pub struct Context<T: Timestamp+Lattice> {
    root: Root<Allocator>,
    input_handle: InputSession<T, Datom, isize>,
    db: DB<T>,
    probe: Option<ProbeHandle<T>>,
}

static mut CTX: Option<Context<usize>> = None;

//
// QUERY PLAN GRAMMAR
//

#[derive(Serialize, Deserialize, Clone)]
pub enum Plan {
    Project(Box<Plan>, Vec<Var>),
    Or(Box<Plan>, Box<Plan>), // @TODO maybe rename Union
    Join(Box<Plan>, Box<Plan>, Var),
    Not(Box<Plan>),
    Lookup(Entity, Attribute, Var),
    Entity(Entity, Var, Var),
    HasAttr(Var, Attribute, Var),
    Filter(Var, Attribute, Value)
}

js_serializable!(Plan);
js_deserializable!(Plan);

type Var = u32;

//
// RELATIONS
//

trait Relation<'a, G: Scope> where G::Timestamp : Lattice {
    fn symbols(&self) -> &Vec<Var>;
    fn tuples(self) -> Collection<G, Vec<Value>, isize>;
    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<G, (Vec<Value>, Vec<Value>), isize>;
}

struct SimpleRelation<G: Scope> where G::Timestamp : Lattice {
    symbols: Vec<Var>,
    tuples: Collection<G, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for SimpleRelation<G> where G::Timestamp : Lattice {
    fn symbols(&self) -> &Vec<Var> { &self.symbols }
    fn tuples(self) -> Collection<G, Vec<Value>, isize> { self.tuples }

    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<G, (Vec<Value>, Vec<Value>), isize>{
        let relation_symbols = self.symbols.clone();
        self.tuples()
            .map(move |tuple| {
                let key = syms.iter()
                    .map(|sym| {
                        let idx = relation_symbols.iter().position(|&v| *sym == v).unwrap();
                        tuple[idx].clone()
                    })
                    .collect();
                (key, tuple)
            })
    }
}

struct VariableRelation<'a, G: Scope> where G::Timestamp : Lattice {
    symbols: Vec<Var>,
    tuples: Variable<'a, G, Vec<Value>, isize>,
}

impl<'a, G: Scope> Relation<'a, G> for VariableRelation<'a, G> where G::Timestamp : Lattice {
    fn symbols(&self) -> &Vec<Var> { &self.symbols }
    fn tuples(self) -> Collection<G, Vec<Value>, isize> { self.tuples.leave() }

    fn tuples_by_symbols(self, syms: Vec<Var>) -> Collection<G, (Vec<Value>, Vec<Value>), isize>{
        let relation_symbols = self.symbols.clone();
        self.tuples()
            .map(move |tuple| {
                let key = syms.iter()
                    .map(|sym| {
                        let idx = relation_symbols.iter().position(|&v| *sym == v).unwrap();
                        tuple[idx].clone()
                    })
                    .collect();
                (key, tuple)
            })
    }
 }

//
// QUERY PLAN IMPLEMENTATION
//
// @TODO return handles to modify the parameters of the query

/// Takes a query plan and turns it into a differential dataflow. The
/// dataflow is extended to feed output tuples to JS clients. A probe
/// on the dataflow is returned.
fn implement<T: Timestamp+Lattice>(plan: Plan, ctx: &mut Context<T>) -> ProbeHandle<T> {
    let db = &mut ctx.db;
    ctx.root.dataflow(|scope| {
        let output_relation = implement_plan(&plan, db, scope);

        output_relation.tuples()
            // .inspect(|&(ref tuple, _x, diff)| {
            //     js! {
            //         __UGLY_DIFF_HOOK(@{tuple}, @{diff as i32}); // @TODO how to get rid of the cast?
            //     }
        // })
            .inspect_batch(move |_t, tuples| {
                let out: Vec<Out> = tuples.into_iter()
                    .map(move |x| Out(x.0.clone(), x.2)) // @FRANK why is this still borrowed content?
                    .collect();
                
                js! {
                    __UGLY_DIFF_HOOK(@{out});
                }
            })
            .probe()
    })
}

fn implement_plan<'a, A: Allocate, T: Timestamp+Lattice>(plan: &Plan, db: &mut DB<T>, scope: &mut Child<'a, Root<A>, T>) -> SimpleRelation<Child<'a, Root<A>, T>> {
    match plan {
        &Plan::Project(ref sub_plan, ref symbols) => {
            let mut relation = implement_plan(sub_plan.deref(), db, scope);
            let tuples = relation
                .tuples_by_symbols(symbols.clone())
                .map(|(key, _tuple)| key);
            
            SimpleRelation { symbols: symbols.to_vec(), tuples }
        },
        &Plan::Or(ref left_plan, ref right_plan) => {
            let mut left = implement_plan(left_plan.deref(), db, scope);
            let mut right = implement_plan(right_plan.deref(), db, scope);

            SimpleRelation {
                // @TODO assert that both relations use the same set of symbols
                symbols: left.symbols().clone(),
                tuples: left.tuples()
                    .concat(&right.tuples())
                    .distinct()
            }
        },
        &Plan::Join(ref left_plan, ref right_plan, join_var) => {
            let mut left = implement_plan(left_plan.deref(), db, scope);
            let mut right = implement_plan(right_plan.deref(), db, scope);

            let symbols = vec![join_var];
            // @TODO correct symbols here
            let mut left_syms = left.symbols().clone();
            let mut right_syms = right.symbols().clone();

            let mut rel_symbols: Vec<Var> = Vec::with_capacity(left_syms.len() + right_syms.len());
            rel_symbols.append(&mut left_syms);
            rel_symbols.append(&mut right_syms);

            let tuples = left.tuples_by_symbols(symbols.clone())
                .arrange_by_key()
                .join_core(&right.tuples_by_symbols(symbols.clone()).arrange_by_key(), |_key, v1, v2| {
                    // @TODO can haz array here?
                    // @TODO avoid allocation, if capacity available in v1
                    let mut vstar = Vec::with_capacity(v1.len() + v2.len());
                    
                    vstar.append(&mut (*v1).clone());
                    vstar.append(&mut (*v2).clone());
                    
                    Some(vstar)                    
                });
            
            SimpleRelation { symbols: rel_symbols, tuples }
        },
        &Plan::Not(ref plan) => {
            // implement_negation(plan.deref(), db, scope)
            
            let mut rel = implement_plan(plan.deref(), db, scope);
            SimpleRelation {
                symbols: rel.symbols().clone(),
                tuples: rel.tuples().negate()
            }
        },
        &Plan::Lookup(e, a, sym1) => {
            let ea_in = scope.new_collection_from(vec![(e, a)]).1.arrange_by_self();
            let tuples = db.ea_v.import(scope)
                .join_core(&ea_in, |_, v, _| {
                    let mut vs: Vec<Value> = Vec::with_capacity(8);
                    vs.push(v.clone());

                    Some(vs)
                });
            
            SimpleRelation { symbols: vec![sym1], tuples }
        },
        &Plan::Entity(e, sym1, sym2) => {
            let e_in = scope.new_collection_from(vec![e]).1.arrange_by_self();
            let tuples = db.e_av.import(scope)
                .join_core(&e_in, |_, &(a, ref v), _| {
                    let mut vs: Vec<Value> = Vec::with_capacity(8);
                    vs.push(Value::Attribute(a));
                    vs.push(v.clone());

                    Some(vs)
                });
            
            SimpleRelation { symbols: vec![sym1, sym2], tuples }
        },
        &Plan::HasAttr(sym1, a, sym2) => {
            let a_in = scope.new_collection_from(vec![a]).1.arrange_by_self();
            let tuples = db.a_ev.import(scope)
                .join_core(&a_in, |_, &(e, ref v), _| {
                    let mut vs: Vec<Value> = Vec::with_capacity(8);
                    vs.push(Value::Eid(e));
                    vs.push(v.clone());
                    
                    Some(vs)
                });
            
            SimpleRelation { symbols: vec![sym1, sym2], tuples }
        },
        &Plan::Filter(sym1, a, ref v) => {
            let av_in = scope.new_collection_from(vec![(a, v.clone())]).1.arrange_by_self();
            let tuples = db.av_e.import(scope)
                .join_core(&av_in, |_, &e, _| {
                    let mut vs: Vec<Value> = Vec::with_capacity(8);
                    vs.push(Value::Eid(e));
                    
                    Some(vs)
                });
            
            SimpleRelation { symbols: vec![sym1], tuples }
        }
    }
}

// fn implement_negation<'a>(plan: &Plan, db: &mut DB, scope: &mut Scope<'a>) -> SimpleRelation<'a> {
//     match plan {
//         &Plan::Lookup(e, a, sym1) => {
//             let ea_in = scope.new_collection_from(vec![(e, a)]).1;
//             let tuples = db.ea_v.import(scope)
//                 .antijoin(&ea_in)
//                 .distinct()
//                 .map(|(_, v)| { vec![v] });
            
//             SimpleRelation { symbols: vec![sym1], tuples }
//         },
//         &Plan::Entity(e, sym1, sym2) => {
//             let e_in = scope.new_collection_from(vec![e]).1;
//             let tuples = db.e_av.import(scope)
//                 .antijoin(&e_in)
//                 .distinct()
//                 .map(|(_, (a, v))| { vec![Value::Attribute(a), v] });
            
//             SimpleRelation { symbols: vec![sym1, sym2], tuples }
//         },
//         &Plan::HasAttr(sym1, a, sym2) => {
//             let a_in = scope.new_collection_from(vec![a]).1;
//             let tuples = db.a_ev.import(scope)
//                 .antijoin(&a_in)
//                 .distinct()
//                 .map(|(_, (e, v))| { vec![Value::Eid(e), v] });
            
//             SimpleRelation { symbols: vec![sym1, sym2], tuples }
//         },
//         &Plan::Filter(sym1, a, ref v) => {
//             let av_in = scope.new_collection_from(vec![(a, v.clone())]).1;
//             let tuples = db.av_e.import(scope)
//                 .antijoin(&av_in)
//                 .distinct()
//                 .map(|(_, e)| { vec![Value::Eid(e)] });
                
//             SimpleRelation { symbols: vec![sym1], tuples }
//         },
//         _ => panic!("Negation not supported for this plan.")
//     }
// }

//
// PUBLIC API
//

#[js_export]
pub fn setup() {
    unsafe {
        let mut root = setup_threadless();
        
        let (input_handle, db) = root.dataflow(|scope| {
            let (input_handle, datoms) = scope.new_collection::<Datom, isize>();
            let db = DB {
                e_av: datoms.map(|Datom(e, a, v)| (e, (a, v))).arrange_by_key().trace,
                a_ev: datoms.map(|Datom(e, a, v)| (a, (e, v))).arrange_by_key().trace,
                ea_v: datoms.map(|Datom(e, a, v)| ((e, a), v)).arrange_by_key().trace,
                av_e: datoms.map(|Datom(e, a, v)| ((a, v), e)).arrange_by_key().trace,
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
pub fn register(plan: Plan) -> bool {
    // @TODO take a string key to distinguish output callbacks
    
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                ctx.probe = Some(implement(plan, ctx));
                true
            }
        }
    }
}

#[js_export]
pub fn transact(tx: usize, d: Vec<TxData>) -> bool {
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                for TxData(op, e, a, v) in d {
                    if op == DB_ADD {
                        ctx.input_handle.insert(Datom(e, a, v));
                    } else if op == DB_RETRACT {
                        ctx.input_handle.remove(Datom(e, a, v));
                    } else {
                        panic!("Unknown operation");
                    }
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

