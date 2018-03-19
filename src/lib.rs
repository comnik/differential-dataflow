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

#[macro_use]
extern crate stdweb;
#[macro_use]
extern crate lazy_static;

use stdweb::js_export;

use std::hash::Hash;

use lattice::Lattice;
use timely::{Root, Allocator};
use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::channels::pact::Pipeline;
use timely::execute::{setup_threadless};

const ATTR_EVENT_TYPE: u32 = 0;

type Datom = u32;//(u64, u32, u64);
type InputHandle = Handle<u64, Datom>;

pub struct Context {
    root: Root<Allocator>,
    input_handle: Option<InputHandle>,
    // output_callbacks,
}

fn make_context() -> Context {
    let root = setup_threadless();
    Context {
        root,
        input_handle: None
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

#[js_export]
pub fn register(computation: u32) -> bool {
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                let mut input = ctx.root.dataflow::<u64,_,_>(|scope| {
                    let (datoms_in, datoms) = scope.new_input();
                    interpret(datoms, computation);
                    
                    datoms_in
                });

                ctx.input_handle = Some(input);
                
                true
            }
        }
    }
}

/// This takes a potentially JS-generated description of a dataflow
/// graph and turns it into one that differential can understand.
fn interpret<G: Scope> (datoms: Stream<G, Datom>, computation: u32) where G::Timestamp: Lattice+Hash+Ord {

    match computation {
        0 => {
            // [_ :event/type _]
            let stream = datoms
                .map(|x| x + 1)
                .sink(Pipeline, "example", |input| {
                    while let Some((time, data)) = input.next() {
                        for datum in data.iter() {
                            js! {
                                var datum = @{datum};
                                __UGLY_DIFF_HOOK(datum);
                            }
                        }
                    }
                });

            // results
            //     .filter(move |&d| d == ATTR_EVENT_TYPE);
                // .filter(move |&(e, a, v)| a == ATTR_EVENT_TYPE);
        }
        1 => {
            // [_ :event/type 400]
            let stream = datoms
                .map(|x| x - 1)
                .sink(Pipeline, "example", |input| {
                    while let Some((time, data)) = input.next() {
                        for datum in data.iter() {
                            js! {
                                var datum = @{datum};
                                __UGLY_DIFF_HOOK(datum);
                            }
                        }
                    }
                });

            // results
            //     .filter(move |&(e, a, v)| a == ATTR_EVENT_TYPE)
            //     .filter(move |&(e, a, v)| v == 400);
        }
        _ => {
            panic!("error: Unknown computation");
        }
    };
}

#[js_export]
pub fn demo() {
    let mut root = setup_threadless();
    let probe = root.dataflow::<(),_,_>(|scope| {
        let stream = (0 .. 9)
            .to_stream(scope)
            .map(|x| x + 1)
            .sink(Pipeline, "example", |input| {
                while let Some((time, data)) = input.next() {
                    for datum in data.iter() {
                        js! {
                            var datum = @{datum};
                            __UGLY_DIFF_HOOK(datum);
                        }
                    }
                }
            });
    });

    root.step();
}

#[js_export]
pub fn send(tx: u64, d: Vec<Datom>) -> bool {
    let mut dmut = d.clone();
    
    unsafe {
        match CTX {
            None => false,
            Some(ref mut ctx) => {
                match ctx.input_handle {
                    None => false,
                    Some(ref mut input) => {
                        input.send_batch(&mut dmut);
                        input.advance_to(tx + 1);
                        
                        ctx.root.step();

                        true
                    }
                }
            }
        }
    }
}
