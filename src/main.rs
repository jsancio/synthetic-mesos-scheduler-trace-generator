#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate docopt;
extern crate rand;
extern crate rustc_serialize;
extern crate time;

use rand::Rng;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;

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
    timestamp: time::Tm,
    framework_id: &'a str,
    task_id: String,
    slave_id : &'a str,
    labels: Vec<Label<'a>>,
}

impl<'a> TaskInfo<'a> {
    fn new<'b>(
        timestamp: time::Tm,
        framework_id: &'b str,
        task_id: String,
        slave_id: &'b str,
        task_type: &'b str,
	task_status: &'b str) -> TaskInfo<'b> {

        TaskInfo {
            timestamp: timestamp,
            framework_id: framework_id,
            task_id: task_id,
            slave_id : slave_id,
            labels: vec![
                Label { key: "TASK_TYPE", value: task_type },
                Label { key: "TASK_STATUS", value: task_status }
	    ],
        }
    }
}

impl<'a> Eq for TaskInfo<'a> {
}

impl<'a> PartialEq for TaskInfo<'a> {
    fn eq(&self, other: &TaskInfo<'a>) -> bool {
        /* Good enough for now but it is not technically correct. This needs to look
	 * at all of the fields
	 */
        self.task_id == other.task_id
    }
}

impl<'a> PartialOrd for TaskInfo<'a> {
    fn partial_cmp(&self, other: &TaskInfo<'a>) -> Option<Ordering> {
        // Hack we want a min-heap
    	other.timestamp.partial_cmp(&self.timestamp)
    }
}

impl<'a> Ord for TaskInfo<'a> {
    fn cmp(&self, other: &TaskInfo<'a>) -> Ordering {
        // Hack we want a min-heap
    	other.timestamp.cmp(&self.timestamp)
    }
}

impl<'a> fmt::Display for TaskInfo<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "{} {} {} {}", self.timestamp.rfc3339(), self.framework_id, self.task_id, self.slave_id));
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

    let launched = "LAUNCHED";
    let finished = "FINISHED";

    let max_events = args.arg_event_count.parse().unwrap();
    let mut now = time::now_utc();

    let mut min_heap: BinaryHeap<TaskInfo> = BinaryHeap::new();

    println!(
        "HEADER: <timestamp> <framework-id> <task-id> <slave-id> [<label-key>:<label-value>]...");
    for _ in 0..max_events {
	now = now + time::Duration::seconds(rng.gen_range(0, 10));

	loop {
            if let Some(task_finished_info) = min_heap.peek() {
	        if task_finished_info.timestamp <= now {
                    println!("{}", task_finished_info);
                } else {
		    break;
		}
            } else {
	        break;
	    }

	    // Didn't exit the loop so pop the top element
	    min_heap.pop();
	}

        let task_id: String = rng.gen_ascii_chars().take(24).collect();

	let task_type = rng.choose(&task_types).unwrap();

        let task_launched_info = TaskInfo::new(
	    now,
            rng.choose(&framework_ids).unwrap(),
            task_id,
            rng.choose(&slave_ids).unwrap(),
	    task_type,
	    &launched);
        println!("{}", task_launched_info);

	// Create the FINISHED event
        min_heap.push(
	    TaskInfo::new(
                now + time::Duration::seconds(rng.gen_range(1, 30)), // TODO: rnd number from 1 to 30
	        task_launched_info.framework_id,
       	        task_launched_info.task_id,
	        task_launched_info.slave_id,
	        task_type,
	        &finished
            )
        );
    }

    // Print all the remaining finished task info in order
    while let Some(task_finished_info) = min_heap.pop() {
        println!("{}", task_finished_info);
    }
}
