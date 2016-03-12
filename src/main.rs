#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate docopt;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

use std::fmt;
use rand::Rng;

/*
 * Tasks:
 * What is the most frequently executed task type?
 * What are the frameworks that executed that task?
 * Which framework executes the most frequent task type?
 *
 * Jobs: Define a job a sequence of 3 tasks by a framework.
 * What is the most frequently executed job?
 * Which frameworks execute the most frequent job?
 *
 */

#[derive(Debug)]
struct Label<'a> {
    key: &'a str,
    value: &'a str,
}


#[derive(Debug)]
struct TaskInfo<'a> {
    framework_id: &'a str,
    task_id: &'a str,
    slave_id : &'a str,
    labels: Vec<Label<'a>>,
}

impl<'a> TaskInfo<'a> {
    fn new<'b>(
        framework_id: &'b str,
        task_id: &'b str,
        slave_id: &'b str,
        task_type: &'b str) -> TaskInfo<'b> {

        TaskInfo {
            framework_id: framework_id,
            task_id: task_id,
            slave_id : slave_id,
            labels: vec![Label { key: "TASK_TYPE", value: task_type }],
        }
    }
}

impl<'a> fmt::Display for TaskInfo<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "{} {} {}", self.framework_id, self.task_id, self.slave_id));
        for &Label { ref key, ref value } in &self.labels {
            try!(write!(f, " {}:{}", key, value));
        }

        Result::Ok(())
    }
}

docopt!(Args, "
Usage:
   mesos-scheduler-trace-gen <event-count> 
");

fn main() {
    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());

    let mut rng = rand::thread_rng();
    let framework_ids: Vec<String> = (0..100)
        .map(|_| rng.gen_ascii_chars().take(24).collect())
        .collect();
    let slave_ids: Vec<String> = (0..100)
        .map(|_| rng.gen_ascii_chars().take(24).collect())
        .collect();
    let task_types: Vec<_> = (0..10)
        .map(|x| format!("TASK_TYPE_{}", x))
        .collect();

    let max_events = args.arg_event_count.parse().unwrap();
    let now = time::now_utc();

    println!(
        "HEADER: <timestamp> <framework-id> <task-id> <slave-id> [<label-key>:<label-value>]...");
    for step in 0..max_events {
        let task_id: String = rng.gen_ascii_chars().take(24).collect();

        let task_info = TaskInfo::new(
            rng.choose(&framework_ids).unwrap(),
            &task_id,
            rng.choose(&slave_ids).unwrap(),
            rng.choose(&task_types).unwrap());
        println!("{} {}", (now + time::Duration::seconds(step)).rfc3339(), task_info);
    }
}
