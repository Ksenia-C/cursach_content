use petgraph::{graph::Graph, stable_graph::NodeIndex, Directed};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, collections::HashSet, fs::File, io::Read, io::Write, path::Path};
// use std::{error::Error, io, process};

#[derive(Serialize, Deserialize)]
struct Instance {
    time: u64,
    cpu_avg: f64,
    cpu_diff_max: f64,
}

type InstDag = Graph<TaskInstInfo, u64, Directed>;

#[derive(Serialize, Deserialize)]
struct TaskInfo {
    name: String,
    instance_cnt: u64,
    start_time: u64,
    end_time: u64,
    dependences: Vec<u32>,
}

#[derive(Serialize, Deserialize)]
struct TaskInstInfo {
    name: String,
    instance_cnt: u64,
    start_time: u64,
    end_time: u64,
    dependences: Vec<u32>,
    instances: Vec<Instance>,
}

type DAG = Graph<TaskInfo, u64, Directed>;

const BATCH_TASK_FILE: &str = "/home/ksenia/vsc/alibaba_dataset/dump_files/batch_task.csv";
const MAX_GRAPHS: i32 = 100000;
const BATCH_INSTANCE_FILENAME: &str =
    "/home/ksenia/vsc/alibaba_dataset/dump_files/batch_instance_.csv";
pub const INS_INPUT_FILENAME: &str = "../dump_files/save_result_ins_0.json";

fn extend_dags(old_dag: &DAG) -> InstDag {
    let mut result = InstDag::new();
    for node in old_dag.node_weights() {
        result.add_node(TaskInstInfo {
            name: node.name.clone(),
            instance_cnt: node.instance_cnt,
            start_time: node.start_time,
            end_time: node.end_time,
            dependences: node.dependences.clone(),
            instances: Vec::new(),
        });
    }
    // result.extend_with_edges(old_dag.raw_edges().iter());
    for edge in old_dag.raw_edges() {
        result.add_edge(edge.source(), edge.target(), edge.weight);
    }
    return result;
}

#[allow(dead_code)]
pub fn main_tasks() {
    let mut jobs = HashMap::<String, Graph<TaskInfo, u64, Directed>>::new();
    let mut task_to_index = HashMap::<String, HashMap<String, NodeIndex>>::new();

    let mut rdr = csv::Reader::from_path(BATCH_TASK_FILE).unwrap();

    let mut unterminated_jobs: HashSet<String> = HashSet::new();
    // let mut max_cnt = MAX_GRAPHS;
    let mut max_cnt: i32 = -1; // for release
    for result in rdr.records() {
        let record = result.expect("a CSV record");
        let dependences: Vec<&str> = record.get(0).unwrap().split('_').collect();

        let task_name = dependences[0].to_string();
        if task_name.starts_with("task") {
            continue;
        }
        let task_name = match task_name[1..].parse::<u64>() {
            Ok(task_number) => {
                format!("task{}", task_number)
            }
            Err(_) => {
                continue;
            }
        };

        let jobs_name = record.get(2).unwrap();
        if unterminated_jobs.contains(jobs_name) {
            continue;
        }
        if !record.get(4).unwrap().eq("Terminated") {
            unterminated_jobs.insert(jobs_name.to_string());
            continue;
        }
        let jobs_name = jobs_name.to_string();

        let mut graph = jobs.get_mut(&jobs_name);
        if graph.is_none() {
            jobs.insert(jobs_name.to_string(), Graph::new());
            graph = jobs.get_mut(&jobs_name);
            task_to_index.insert(jobs_name.to_string(), HashMap::new());
        }

        let job_indexs = task_to_index.get_mut(&jobs_name).unwrap();
        let graph = graph.expect("Failed to create new job");

        let ver_info = TaskInfo {
            name: task_name.to_string(),
            instance_cnt: record.get(1).unwrap().parse().unwrap(),
            start_time: record.get(5).unwrap().parse().unwrap(),
            end_time: record.get(6).unwrap().parse().unwrap(),
            dependences: dependences
                .iter()
                .skip(1)
                .filter_map(|str| match str.parse::<u32>() {
                    Ok(num) => Some(num),
                    Err(_) => None,
                })
                .collect(),
        };

        // we could insert empty nodes if some node apeared before its dependances
        let task_index: NodeIndex = if job_indexs.contains_key(&task_name) {
            let task_index = job_indexs[&task_name];
            let node_wight = graph.node_weight_mut(task_index).unwrap();
            node_wight.instance_cnt = ver_info.instance_cnt;
            node_wight.start_time = ver_info.start_time;
            node_wight.end_time = ver_info.end_time;
            node_wight.dependences = ver_info.dependences;
            task_index
        } else {
            let task_index = graph.add_node(ver_info);
            job_indexs.insert(task_name.to_string(), task_index);
            task_index
        };

        for dependance in dependences.iter().skip(1) {
            if dependance.len() == 0 {
                continue;
            }

            let dependance_name = match dependance.parse::<u64>() {
                Ok(dependance_num) => {
                    format!("task{}", dependance_num)
                }
                Err(_) => {
                    unterminated_jobs.insert(jobs_name);
                    break;
                }
            };

            if !job_indexs.contains_key(&dependance_name) {
                let empty_info = TaskInfo {
                    name: dependance_name.to_string(),
                    instance_cnt: 0,
                    start_time: 0,
                    end_time: 0,
                    dependences: Vec::new(),
                };

                let empty_task_index = graph.add_node(empty_info);
                job_indexs.insert(dependance_name.to_string(), empty_task_index);
            }

            graph.add_edge(job_indexs[&dependance_name], task_index, 1);
        }
        if max_cnt > 0 {
            // for debug
            max_cnt -= 1;
            if max_cnt == 0 {
                break;
            }
            println!("till end {}", max_cnt);
        }
    }

    println!("unterminated: {}", unterminated_jobs.len());
    for unterminate_job in unterminated_jobs.iter() {
        jobs.remove(unterminate_job);
        task_to_index.remove(unterminate_job);
    }

    println!("Stayed jobs: {}", jobs.len());
    let j = serde_json::to_string(&jobs).unwrap();

    let path = Path::new("../save_result.json");
    let mut file = match File::create(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };

    match file.write_all(j.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }

    let path = Path::new("../save_indexes.json");

    let mut file = match File::create(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };

    let j = serde_json::to_string(&task_to_index).unwrap();
    match file.write_all(j.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }
}

type TaskIndexesT = HashMap<String, HashMap<String, NodeIndex>>;

fn get_task_indexes() -> TaskIndexesT {
    let path = Path::new("../save_indexes.json");
    let mut file = match File::open(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };
    let mut contents = String::new();
    match file.read_to_string(&mut contents) {
        Err(why) => panic!("couldn't read: {}", why),
        Ok(_) => {}
    }
    return serde_json::from_str(&contents).unwrap();
}
fn get_graphs(filename: &str) -> HashMap<String, DAG> {
    let path = Path::new(filename);
    let mut file = match File::open(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };
    let mut contents = String::new();
    match file.read_to_string(&mut contents) {
        Err(why) => panic!("couldn't read: {}", why),
        Ok(_) => {}
    }
    return serde_json::from_str(&contents).unwrap();
}

pub fn main_instances() {
    let jobs = get_graphs("../save_result.json");
    let mut task_to_index = get_task_indexes();

    // they broke my code because of wrong task_name format
    let pass_jobs = vec![
        "j_4015961",
        "j_1127318",
        "j_2583771",
        "j_3734942",
        "j_2212822",
        "j_1465012",
        "j_1053726",
        "j_3061955",
        "j_2123594",
        "j_94055",
        "j_2428957",
        "j_2598590",
        "j_1575128",
    ];
    let mut rdr = csv::Reader::from_path(BATCH_INSTANCE_FILENAME).unwrap();

    println!("real work start");
    let mut unterminated_jobs: HashSet<String> = HashSet::new();

    // let mut max_cnt = MAX_GRAPHS;
    let mut max_cnt: i32 = -1; // for release

    let mut lines = 0;
    let mut just_pass = 0;

    let mut jobs_with_instances = HashMap::<String, InstDag>::new();
    for result in rdr.records() {
        let record = result.expect("a CSV record");
        lines += 1;
        if lines % 1000000 == 0 {
            println!("overal lines {}", lines);
        }
        // if lines % 200 == 0 {
        //     println!("overal lines {}", lines);
        // }
        // if lines > 80000000 {
        //     break;
        // }

        let job_name = record.get(2).unwrap();

        if !job_name[2..].parse::<u32>().unwrap() / 200000 == 0 {
            continue;
        }

        if pass_jobs.contains(&job_name) {
            continue;
        }
        if unterminated_jobs.contains(job_name) {
            continue;
        }
        let job_ins = jobs.get(job_name);
        if job_ins.is_none() {
            just_pass += 1;
            if just_pass % 100000 == 0 {
                println!("pass not found {}", just_pass);
            }
            continue;
        }
        let job_ins = job_ins.unwrap();

        let dependences: Vec<&str> = record.get(1).unwrap().split('_').collect();

        let task_name = dependences[0].to_string();
        if task_name.starts_with("task") {
            continue;
        }
        let task_name = match task_name[1..].parse::<u64>() {
            Ok(task_number) => {
                format!("task{}", task_number)
            }
            Err(err) => {
                println!("{} {:?}", task_name, err);
                continue;
            }
        };

        let task_ind = task_to_index[job_name].get(&task_name);
        if task_ind.is_none() {
            println!("failed to find task {} for job {}", task_name, job_name);
            continue;
        }
        let task_ind = task_ind.unwrap();

        if !record.get(4).unwrap().eq("Terminated") {
            unterminated_jobs.insert(job_name.to_string());
            continue;
        }

        let start_time = record.get(5).unwrap().parse::<u64>().unwrap();
        let end_time = record.get(6).unwrap().parse::<u64>().unwrap();
        if end_time < start_time {
            println!("{:?}", record);
            panic!("start {} < end {}", start_time, end_time);
        }
        let mut ins_duraction = end_time - start_time;

        if !jobs_with_instances.contains_key(job_name) {
            jobs_with_instances.insert(job_name.to_string(), extend_dags(&job_ins));
        }
        let graph = jobs_with_instances.get_mut(job_name).unwrap();
        let avg_cpu_sage = match record.get(10).unwrap().parse::<f64>() {
            Ok(avg_cpu_sage) => avg_cpu_sage,
            Err(_) => {
                unterminated_jobs.insert(job_name.to_string());
                continue;
            }
        };
        let max_cpu_sage = match record.get(11).unwrap().parse::<f64>() {
            Ok(max_cpu_sage) => max_cpu_sage,
            Err(_) => {
                unterminated_jobs.insert(job_name.to_string());
                continue;
            }
        };

        // Logic Todo check if use that
        let cpu_cnt = (avg_cpu_sage + 99.0) / 100.0;
        ins_duraction *= (cpu_cnt * 0.8).round() as u64;

        match graph.node_weight_mut(*task_ind) {
            None => {
                panic!(
                    "somehow job {} has no correct index for task {}: {:?}",
                    job_name, task_name, task_ind
                );
            }

            Some(task_info) => task_info.instances.push(Instance {
                time: ins_duraction,
                cpu_avg: avg_cpu_sage,
                cpu_diff_max: max_cpu_sage - avg_cpu_sage,
            }),
        }
        if max_cnt > 0 {
            max_cnt -= 1;
            if max_cnt % 1000000 == 0 {
                println!("from start {}", MAX_GRAPHS - max_cnt);
            }
            if max_cnt == 0 {
                // debug
                break;
            }
        }
    }
    println!("unterminated: {}", unterminated_jobs.len());
    for unterminate_job in unterminated_jobs.iter() {
        jobs_with_instances.remove(unterminate_job);
        task_to_index.remove(unterminate_job);
    }

    println!("Stayed jobs: {}", jobs_with_instances.len());
    let j = serde_json::to_string(&jobs_with_instances).unwrap();

    let path = Path::new(INS_INPUT_FILENAME);
    let mut file = match File::create(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };

    match file.write_all(j.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }
}
