fn implement_join<R: Relation> (ctx: &mut Context, mut rel1: R, syms: Vec<Var>, mut rel2: R) -> SimpleRelation {
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
    
    SimpleRelation { symbols: syms, tuples: rel }
}

fn parse_graph(ctx: &mut Context, mut clauses: Vec<Clause>) -> ProbeHandle {
  
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
