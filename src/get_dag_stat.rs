use crate::pure_dag::*;
use crate::task_dag::*;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn current_measure(file_path: &str, graph: &TaskDag, str_bufer: &mut String) {
    if file_path.ends_with("sparity") {
        str_bufer.push_str(&format!("{} ", graph.sparity()));
    } else if file_path.ends_with("chain_ration") {
        str_bufer.push_str(&format!("{} ", graph.chain_ratio()));
    } else if file_path.ends_with("in_deg") {
        for deg in graph.in_degree() {
            str_bufer.push_str(&format!("{} ", deg));
        }
    } else if file_path.ends_with("out_deg") {
        for deg in graph.out_degree() {
            str_bufer.push_str(&format!("{} ", deg));
        }
    } else if file_path.ends_with("ins_ratio") {
        for deg in graph.pairwise_ins_ration() {
            str_bufer.push_str(&format!("{} ", deg));
        }
    } else if file_path.ends_with("time_ratio") {
        for deg in graph.pairwise_flops_ration() {
            str_bufer.push_str(&format!("{} ", deg));
        }
    } else if file_path.ends_with("wide_dependen") {
    } else if file_path.ends_with("narrow_dependen") {
    } else {
        panic!("not implemented");
    }
}

pub fn stat_pure_dags(tt_input_dir: String, output_file: &str) {
    // Examples of calc stat
    let paths = fs::read_dir(tt_input_dir).unwrap();

    let path = Path::new(output_file);
    let mut file = match File::create(&path) {
        Err(why) => panic!("cant open file to write {}", why),
        Ok(file) => file,
    };

    let mut str_bufer = String::new();
    let mut graphs_count: u64 = 0;
    for path in paths {
        let path = path.unwrap().path().display().to_string();

        let mut pure_dags = PureDags::get_from_file(path.as_str());
        println!("Real work just starts");

        for (_job_name, graph) in pure_dags.dags.iter_mut() {
            let graph = <TaskDag as TaskDagFuncs>::from_pure_dag(&graph);
            current_measure(output_file, &graph, &mut str_bufer);
            graphs_count += 1;
        }
    }
    match file.write_all(str_bufer.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }
    println!("overal graphs: {}", graphs_count);
}

pub fn stat_task_dags(tt_input_dir: String, output_file: &str) {
    let paths = fs::read_dir(tt_input_dir).unwrap();

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

        let mut task_dag = TaskDag::new();
        task_dag.load_from_file(path.as_str());

        current_measure(output_file, &task_dag, &mut str_bufer);
    }
    match file.write_all(str_bufer.as_bytes()) {
        Err(why) => panic!("cant save serialization {}", why),
        Ok(_) => {}
    }
}
