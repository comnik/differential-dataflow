// import diff from "./differential_dataflow.js"

__UGLY_DIFF_HOOK = (tuple) => {
  console.log('\t=>', tuple)
}

function main () {
  Rust.differential_dataflow.then(diff => {
    diff.setup()
    // diff.register(0)

    window.dd = diff
    console.log('DD ready')
  })
}

function test () {
  dd.setup()
  dd.register([{HasAttr: [0, 300, 1]}, {HasAttr: [0, 100, 2]}])
  dd.send(0, [
    {e: 1, a: 100, v: {Number: 9012}},
    {e: 1, a: 300, v: {Number: 123}},
    {e: 2, a: 100, v: {Number: 888}},
    {e: 2, a: 200, v: {Number: 111}},
  ])
}

window.test = test

main()
