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

main()
