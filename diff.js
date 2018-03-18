// import diff from "./differential_dataflow.js"

__UGLY_DIFF_HOOK = (datum) => {
  console.log("nah", datum)
}

function main () {
  Rust.differential_dataflow.then(diff => {
    diff.register(0)
  })
}

main()
