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
    [1, 100, {Number: 9012}],
    [1, 300, {Number: 123}],
    [2, 100, {Number: 888}],
    [2, 200, {Number: 111}],
  ])
}

window.test = test

main()
