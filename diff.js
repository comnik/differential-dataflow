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

const samplePlans = [
  {Join: [
    {HasAttr: [0, 300, 1]},
    {HasAttr: [0, 100, 2]},
    0]},
  {Join: [
    {HasAttr: [0, 200, 1]},
    {HasAttr: [0, 100, 2]},
    0]},
]

function test (i=0) {
  dd.setup()
  dd.register(samplePlans[i])
  dd.send(0, [
    [1, 100, {Number: 9012}],
    [1, 300, {Number: 123}],
    [2, 100, {Number: 888}],
    [2, 200, {Number: 111}],
  ])
}

window.test = test

main()
