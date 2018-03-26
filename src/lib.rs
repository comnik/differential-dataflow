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
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::probe::Handle;
// use timely::dataflow::channels::pact::Pipeline;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::execute::{setup_threadless};

use input::{Input, InputSession};
use trace::{Trace};
use trace::implementations::ord::OrdValBatch;
use trace::implementations::spine::Spine;
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf, TraceAgent};
use operators::join::JoinCore;

const ATTR_NAME: u32 = 100;
const ATTR_FRIEND: u32 = 200;
const ATTR_AGE: u32 = 300; 

const VALUE_NAME_DIPPER: u64 = 100;
const VALUE_NAME_MABEL: u64 = 101;

//
// TYPES
//

type Entity = u64;
type Attribute = u32;

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug, Serialize, Deserialize)]
enum Value {
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

// impl Ord for Datom {
//     fn cmp(&self, other: &Datom) -> Ordering {
//         self.e.cmp(&other.e)
//             .then(self.a.cmp(&other.a))
//             .then(self.v.cmp(&other.v))
//     }
// }

// impl PartialOrd for Datom {
//     fn partial_cmp(&self, other: &Datom) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl PartialEq for Datom {
//     fn eq(&self, other: &Datom) -> bool {
//         self.e == other.e
//     }
// }

type Scope<'a> = Child<'a, Root<Allocator>, usize>;
type RootTime = Product<RootTimestamp, usize>;
type Probe = Handle<RootTime>;
type InputHandle = InputSession<usize, Datom, isize>;
// type TraceBatch = OrdValBatch<usize, usize, RootTime, isize>;
// type TraceSpine = Spine<usize, usize, RootTime, isize, Rc<TraceBatch>>;
// type TraceHandle = TraceAgent<usize, usize, RootTime, isize, TraceSpine>;
type Index<'a, K, V> = Arranged<Scope<'a>, K, V, isize,
                                TraceAgent<K, V, RootTime, isize,
                                           Spine<K, V, RootTime, isize,
                                                 Rc<OrdValBatch<K, V, RootTime, isize>>>>>;

//
// CONTEXT
//

struct DB {
    e_av: Index<'static, Entity, (Attribute, Value)>,
    a_ev: Index<'static, Attribute, (Entity, Value)>,
    ea_v: Index<'static, (Entity, Attribute), Value>,
    av_e: Index<'static, (Attribute, Value), Entity>,
}

pub struct Context {
    root: Root<Allocator>,
    input_handle: Option<InputHandle>,
    probe: Option<Probe>,
    db: Option<DB>,
    // output_callbacks,
}

fn make_context() -> Context {
    let root = setup_threadless();
    Context {
        root,
        input_handle: None,
        probe: None,
        db: None,
    }
}

static mut CTX: Option<Context> = None;

#[js_export]
pub fn setup() -> bool {
    unsafe {
        CTX = Some(make_context());
        true
    }
}

// #[js_export]
pub fn register(computation: u32) -> bool {
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                let (mut input, mut datoms, mut probe) = ctx.root.dataflow::<usize,_,_>(|mut scope| {
                    let (datoms_in, datoms) = scope.new_collection::<>();
                    let probe = parse_graph(ctx, &mut scope, &datoms, computation);
                    
                    (datoms_in, datoms, probe)
                });

                ctx.db = Some(DB {
                    e_av: datoms.map(|(e, a, v)| (e, (a, v))).arrange_by_key(),
                    a_ev: datoms.map(|(e, a, v)| (a, (e, v))).arrange_by_key(),
                    ea_v: datoms.map(|(e, a, v)| ((e, a), v)).arrange_by_key(),
                    av_e: datoms.map(|(e, a, v)| ((a, v), e)).arrange_by_key(),
                });
                
                ctx.input_handle = Some(input);
                ctx.probe = Some(probe);
                
                true
            }
        }
    }
}

// struct Relation {
//     in: Vec<u32>,
//     out: Vec<u32>,
// }

//
// QUERY GRAMMAR
//

struct Placeholder;
struct Var(u32);

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug)]
struct Const(Value);

struct LookupPattern(Const, Const, Var);
struct EntityPattern(Const, Var, Var);
struct HasAttrPattern(Var, Const, Var);
struct FilterPattern(Var, Const, Const);

enum Clause {
    Lookup(LookupPattern),
    Entity(EntityPattern),
    HasAttr(HasAttrPattern),
    Filter(FilterPattern),
}

struct Unification { a: Clause, b: Clause, }

//
// INTERPRETER
// 

trait Interpretable<T> {
    fn interpret(&self, ctx: &mut Context, scope: &mut Scope) -> T;
}

// impl Interpretable<Collection<Scope, Value>> for LookupPattern {
//     fn interpret(&self, ctx: &mut Context, scope: &mut Scope) -> Collection<Scope, Value> {
//         let &LookupPattern(e, a, v) = self;
//         let ea_in = scope.new_collection_from(vec![(e, a)]).1.arrange_by_self();
//         ctx.db.unwrap().ea_v.join_core(&ea_in, |&(e, a), v, _| Some(v)).arrange_by_self()
//     }
// }

// impl Interpretable<Collection<Scope, (Attribute, Value)>> for EntityPattern {
//     fn interpret(&self, ctx: &mut Context, scope: &mut Scope) -> Collection<Scope, (Attribute, Value)> {
//         let &EntityPattern(e, a, v) = self;
//         let e_in = scope.new_collection_from(vec![e]).1.arrange_by_self();
//         ctx.db.unwrap().e_av.join_core(&e_in, |e, &(a, v), _| Some((a, v))).arrange_by_key()
//     }
// }

impl<'a> Interpretable<Index<'a, (Attribute, Value), ()>> for HasAttrPattern {
    fn interpret(&self, ctx: &mut Context, scope: &mut Scope) -> Index<'a, (Attribute, Value), ()> {
        let &HasAttrPattern(e, a, v) = self;
        let a_in = scope.new_collection_from(vec![a]).1.arrange_by_self();
        ctx.db.unwrap().a_ev.join_core(&a_in, |a, &(e, v), _| Some((e, v))).arrange_by_key()
    }
}

// impl Interpretable<Collection<Scope, (Attribute, Value)>> for FilterPattern {
//     fn interpret(&self, ctx: &mut Context, scope: &mut Scope) -> Collection<Scope, (Attribute, Value)> {
//         let &FilterPattern(e, a, v) = self;
//         let av_in = scope.new_collection_from(vec![(a, v)]).1.arrange_by_self();
//         ctx.db.unwrap().av_e.join_core(&av_in, |&(a, v), e, _| Some(e)).arrange_by_self()
//     }
// }

// make an input...
// scope.new_collection_from(vec![v]).1.arrange_by_self(),


/// This takes a potentially JS-generated description of a dataflow
/// graph and turns it into one that differential can understand.
fn parse_graph<'a> (ctx: &mut Context, scope: &mut Child<'a, Root<Allocator>, usize>, datoms: &Collection<Scope, Datom>, computation: u32) -> Probe {

    // Given a query...
    // [?e :name ?name] [?e2 :name ?name]
    let clause1 = HasAttrPattern(Var(0), Const(Value::Attribute(ATTR_NAME)), Var(1));
    let clause2 = HasAttrPattern(Var(2), Const(Value::Attribute(ATTR_NAME)), Var(1));

    // We notice that two clauses share the same symbol, therfore we
    // must unify them.
    // let unified = Unification { a: clause1, b: clause2 };
    
    let r1 = clause1.interpret(ctx, scope);
    let r2 = clause2.interpret(ctx, scope);

    // implicit join
    // let r3 = r2.join_core(&r1, |e, &v1, &v2| Some((*e, v1)))
    
    let probe = r2
        .inspect(|res| {
            let (e, v) = res.0;
            js! {
                var datom = @{JsDatom{e, a: ATTR_NAME, v}};
                __UGLY_DIFF_HOOK(datom);
            }
        })
        .probe();

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
                match ctx.input_handle {
                    None => false,
                    Some(ref mut input) => {
                        for js_datom in d {
                            input.insert((js_datom.e, js_datom.a, js_datom.v));
                        }
                        input.advance_to(tx + 1);
                        input.flush();

                        match ctx.probe {
                            None => false,
                            Some(ref mut probe) => {
                                while probe.less_than(input.time()) {
                                    ctx.root.step();
                                }

                                true
                            }
                        }
                    }
                }
            }
        }
    }
}

//
// TODOS
//
// @TODO define interpretation in terms of Collection, rather than Arrange
// @TODO inspect -> inspect_batch?
