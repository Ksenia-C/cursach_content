use crate::instance::*;
use crate::pure_dag::*;
use crate::task_dag::*;
use rand::rngs::ThreadRng;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;
const ranges: [(usize, usize); 6] = [(2, 4), (5, 7), (8, 10), (11, 14), (15, 18), (19, 24)];

fn init_chars() -> (Vec<InstGrapgChar>, Vec<InstGrapgChar>) {
    let mut init = InstGrapgChar {
        depth: 0,
        max_data: 0.0,
        max_work: 0.0,
        paralel: 0.0,
        tasks_cnt: 0,
        width: 0,
    };
    let mut hash_maxs = vec![init; ranges.len()];
    init = InstGrapgChar {
        depth: usize::MAX,
        max_data: f64::MAX,
        max_work: f64::MAX,
        paralel: f64::MAX,
        tasks_cnt: usize::MAX,
        width: usize::MAX,
    };
    let mut hash_mins = vec![init.clone(); ranges.len()];
    (hash_mins, hash_maxs)
}

fn current_measure(
    graph: &InstanceDag,
    hash_mins: &mut Vec<InstGrapgChar>,
    hash_maxs: &mut Vec<InstGrapgChar>,
) {
    let stat = graph.get_all();
    for (ind, range) in ranges.iter().enumerate() {
        if range.0 <= stat.depth && stat.depth <= range.1 {
            let mins = hash_mins.get_mut(ind).unwrap();
            let maxs = hash_maxs.get_mut(ind).unwrap();

            mins.tasks_cnt = mins.tasks_cnt.min(stat.tasks_cnt);
            mins.width = mins.width.min(stat.width);
            mins.paralel = mins.paralel.min(stat.paralel);
            mins.max_work = mins.max_work.min(stat.max_work);
            mins.max_data = mins.max_data.min(stat.max_data);

            maxs.tasks_cnt = maxs.tasks_cnt.max(stat.tasks_cnt);
            maxs.width = maxs.width.max(stat.width);
            maxs.paralel = maxs.paralel.max(stat.paralel);
            maxs.max_work = maxs.max_work.max(stat.max_work);
            maxs.max_data = maxs.max_data.max(stat.max_data);
        }
    }
}

pub fn char_pure_dags(tt_input_dir: &String, graph_type: &str, output_file: &str) {
    // Examples of calc stat
    let paths = fs::read_dir(tt_input_dir).unwrap();

    let path = Path::new(output_file);
    let mut file = match File::create(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };
    let (mut hash_mins, mut hash_maxs) = init_chars();

    for (ind, hm) in hash_mins.iter_mut().enumerate() {
        hm.depth = ranges[ind].0;
    }
    for (ind, hm) in hash_maxs.iter_mut().enumerate() {
        hm.depth = ranges[ind].1;
    }

    let mut rnd = rand::thread_rng();

    let mut str_bufer = String::new();
    for path in paths {
        let path = path.unwrap().path().display().to_string();
        if !path.contains(graph_type) {
            continue;
        }

        let mut pure_dags = PureDags::get_from_file(path.as_str());
        println!("Real work just starts");

        for (_job_name, graph) in pure_dags.dags.iter_mut() {
            graph.sort_node_ids();

            let graph = <TaskDag as TaskDagFuncs>::from_pure_dag(&graph);
            current_measure(
                &graph.convert_to_inst_dag(&mut rnd, 11.0),
                &mut hash_mins,
                &mut hash_maxs,
            );
        }
    }

    for (hmin, hmax) in hash_mins.iter_mut().zip(hash_maxs.iter_mut()) {
        str_bufer += &format!(
            "{}_real,{}-{},{}-{},{}-{},{}-{},{}-{},{}-{}\n",
            graph_type,
            hmin.tasks_cnt,
            hmax.tasks_cnt,
            hmin.depth,
            hmax.depth,
            hmin.width,
            hmax.width,
            hmin.paralel,
            hmax.paralel,
            hmin.max_work,
            hmax.max_work,
            hmin.max_data,
            hmax.max_data
        );
    }
    match file.write_all(str_bufer.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }
}

pub fn char_task_dags(tt_input_dir: String, graph_type: &str, output_file: &str) {
    let paths = fs::read_dir(tt_input_dir).unwrap();
    let mut rnd = rand::thread_rng();
    let (mut hash_mins, mut hash_maxs) = init_chars();
    for (ind, hm) in hash_mins.iter_mut().enumerate() {
        hm.depth = ranges[ind].0;
    }
    for (ind, hm) in hash_maxs.iter_mut().enumerate() {
        hm.depth = ranges[ind].1;
    }

    let path = Path::new(output_file);
    let mut file = match File::create(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };

    let mut str_bufer = String::new();

    for path in paths {
        let path = path.unwrap().path().display().to_string();

        if path.ends_with("dot") {
            continue;
        }
        println!("{}", path);

        let mut task_dag = TaskDag::new();
        task_dag.load_from_file(path.as_str());

        current_measure(
            &task_dag.convert_to_inst_dag(&mut rnd, 11.0),
            &mut hash_mins,
            &mut hash_maxs,
        );
    }

    for (hmin, hmax) in hash_mins.iter_mut().zip(hash_maxs.iter_mut()) {
        str_bufer += &format!(
            "{}_gen,{}-{},{}-{},{}-{},{}-{},{}-{},{}-{}\n",
            graph_type,
            hmin.tasks_cnt,
            hmax.tasks_cnt,
            hmin.depth,
            hmax.depth,
            hmin.width,
            hmax.width,
            hmin.paralel,
            hmax.paralel,
            hmin.max_work,
            hmax.max_work,
            hmin.max_data,
            hmax.max_data
        );
    }
    match file.write_all(str_bufer.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }
}
