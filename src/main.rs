#![feature(map_try_insert)]
#![feature(assert_matches)]

mod queue;
use queue::Queue;

use std::{
    collections::HashMap,
    result::Result,
    time::{SystemTime, UNIX_EPOCH},
};

use chrono::{serde::ts_seconds_option, DateTime, Utc};

use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum Parameter {
    Const(String),
    Job(DBID),
}

#[derive(Default, Debug)]
struct Workflow {
    jobs: Vec<DBID>,
}
impl Workflow {
    fn queue_queuable(self: &Self, state: &mut State) -> Result<(), ()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        for job_id in self.jobs.iter() {
            let job = state.jobs.get(job_id).unwrap();
            if job.state == JobState::NotStarted
                && job.arguments.iter().all(|argument| match argument {
                    Parameter::Const(_) => true,
                    Parameter::Job(argumgent_id) => {
                        state.jobs.get(argumgent_id).unwrap().state == JobState::Finished
                    }
                })
            {
                // ToDo: Bit of a smell here as inside the functions we do another lookup of the job
                if job.delay_until.is_some_and(|d| d > now) {
                    state.delay_job_from_id(job_id, job.delay_until.unwrap())?;
                } else {
                    state.add_job_from_id(job_id)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Default, Debug, PartialEq)]
enum JobState {
    #[default]
    NotStarted,
    Delayed,
    Waiting,
    Finished,
}

#[derive(Default, Debug)]
struct Job {
    id: DBID,
    job_name: String,
    arguments: Vec<Parameter>,
    priority: Priority,
    state: JobState,
    delay_until: Option<TimeStamp>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum InputParam {
    Const(String),
    Job(InputJob),
}

#[derive(Deserialize, Debug)]
struct InputJob {
    job_name: String,
    parameters: Vec<InputParam>,
    priority: Priority,
    #[serde(with = "ts_seconds_option", default)]
    delay_until: Option<DateTime<Utc>>,
}

type DBID = u128;
type TimeStamp = u64;
type Priority = u8;

#[derive(Default)]
struct State {
    jobs: HashMap<DBID, Job>,
    queue: Queue,
}

impl State {
    fn register_job(self: &mut Self, job: Job) -> Result<DBID, ()> {
        self.jobs
            .try_insert(job.id, job)
            .map_err(|_| ())
            .map(|j| j.id)
    }

    fn add_job_from_id(self: &mut Self, job_id: &DBID) -> Result<(), ()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let job = self.jobs.get_mut(&job_id).ok_or(())?;
        job.state = JobState::Waiting;
        self.queue.add_job(*job_id, job.priority, now)
    }

    fn delay_job_from_id(self: &mut Self, job_id: &DBID, execute_at: TimeStamp) -> Result<(), ()> {
        let job = self.jobs.get_mut(job_id).ok_or(())?;
        job.state = JobState::Delayed;
        self.queue.delay_job(*job_id, job.priority, execute_at)
    }

    fn finish_job(self: &mut Self, job_id: &DBID) -> Result<(), ()> {
        if let Some(job) = self.jobs.get_mut(job_id) {
            job.state = JobState::Finished;
            Ok(())
        } else {
            Err(())
        }
    }

    fn pop(self: &mut Self) -> Result<Option<DBID>, ()> {
        // ToDo this if should go
        if self.queue.is_empty() {
            return Err(());
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.queue
            .enqueue_from_delayed(now)
            .iter()
            .for_each(|enqueued| {
                let job = self.jobs.get_mut(&enqueued).unwrap();
                job.state = JobState::Waiting;
            });

        Ok(self.queue.pop())
    }
}

static mut CUR_ID: DBID = 0;
fn build_workflow_from_job(s: &mut State, job: &InputJob, workflow: &mut Workflow) -> DBID {
    let id = unsafe {
        CUR_ID += 1;
        CUR_ID
    };

    let params = job
        .parameters
        .iter()
        .map(|param| match param {
            InputParam::Job(job) => Parameter::Job(build_workflow_from_job(s, job, workflow)),
            InputParam::Const(str) => Parameter::Const(str.clone()),
        })
        .collect();

    s.register_job(Job {
        id,
        job_name: job.job_name.clone(),
        priority: job.priority,
        arguments: params,
        delay_until: job.delay_until.map(|ts| ts.timestamp() as u64),
        state: Default::default(),
    })
    .unwrap();
    workflow.jobs.push(id);
    return id;
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut s: Box<State> = Box::default();
    let json = r#"
        {
            "job_name": "adsdad",
            "parameters": [
                {
                    "job_name": "adsdad",
                    "parameters": [
                        "boop"
                    ],
                    "priority": 1
                },
                {
                    "job_name": "adsdad",
                    "parameters": [
                        "boop"
                    ],
                    "priority": 2,
                    "delay_until": 1727973409
                }
            ],
            "priority": 9
        }
    "#;
    let workflow = {
        let mut workflow = Default::default();
        let inp: InputJob = serde_json::from_str(&json)?;
        build_workflow_from_job(&mut s, &inp, &mut workflow);
        workflow.queue_queuable(&mut s).unwrap();

        println!("{:?}", s.queue);
        workflow
    };

    while let Ok(job) = s.pop() {
        if let Some(key) = job {
            let job = s.jobs.get(&key).unwrap();
            println!("job_name: {}", job.job_name);
            for a in job.arguments.iter() {
                match a {
                    Parameter::Job(param_key) => {
                        println!(" - Job {param_key}");
                    }
                    Parameter::Const(str) => {
                        println!(" - {str}");
                    }
                }
            }
            s.finish_job(&key).unwrap();
            workflow.queue_queuable(&mut s).unwrap();
            println!("{:?}", s.queue);
        }
    }

    Ok(())
}
