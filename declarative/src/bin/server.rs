use std::io::{Write, BufRead};

fn main () {
    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();

    let mut done = false;

    while !done {

        if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
            let elts: Vec<_> = line.split_whitespace().map(|x| x.to_owned()).collect();

            if elts.len() > 0 {
                match elts[0].as_str() {
                    "help" => { println!("valid commands are currently: help, register, exit"); },
                    "register" => { println!("register"); },
                    "exit" => { done = true; },
                    _ => { println!("unrecognized command: {:?}", elts[0]); },
                }
            }

            std::io::stdout().flush().unwrap();
        }
    }

    println!("main: exited command loop");
}
