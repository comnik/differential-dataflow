// import diff from "./differential_dataflow.js"

__UGLY_DIFF_HOOK = (tuple) => {
  console.log('\t=>', tuple)
}

function main () {
  Rust.differential_dataflow.then(diff => {
    diff.setup()
    window.dd = diff
    console.log('DD ready')
  })
}

const samplePlans = [
  {HasAttr: [0, 100, 1]},
  {Filter: [0, 100, {Number: 888}]},
  {Project: [
    {Join: [
      {HasAttr: [0, 300, 1]},
      {HasAttr: [0, 100, 2]},
      0]},
    [0, 1, 2]]},
  {Join: [
    {HasAttr: [0, 200, 1]},
    {HasAttr: [0, 100, 2]},
    0]},
  {Project: [
    {Join: [
      {HasAttr: [0, 200, 1]},
      {HasAttr: [2, 100, 1]},
      1]},
    [0, 2]]},
  {Project: [
    {Join: [
      {Join: [
	{HasAttr: [0, 200, 1]},
	{HasAttr: [2, 100, 1]},
	1]},
      {Filter: [0, 100, {Number: 888}]},
      0]},
    [0, 2, 1]]},
]

function test (i=0) {
  dd.setup()
  dd.register(samplePlans[i])
  dd.send(0, [
    [1, 100, {Number: 9012}],
    [1, 300, {Number: 123}],
    [2, 100, {Number: 888}],
    [2, 200, {Number: 111}],
    [3, 100, {Number: 1}],
    [3, 200, {Number: 1}],
    [4, 100, {Number: 111}]
  ])
}

window.test = test

main()
