use crate::instance::*;
use crate::pure_dag::{AbsorbStat, DoTraverse, PureDag, PureDags};
use crate::statistic::*;
use crate::task_dag::*;
use petgraph::stable_graph::NodeIndex;
use rand::prelude::SliceRandom;

use rand::Rng;
pub mod instance;
pub mod pure_dag;
pub mod statistic;
pub mod task_dag;

// const TT_INPUT_FILENAME: &str = "../by_graph_type/tree_incr.json";
const CP_RANGES_FILENAME: &str = "cp_ranges.json";
const LEVEL_DISTRIB_FILENAME: &str = "level_distribute.json";
const LEVEL_GENERAOTR_FILENAME: &str = "level_generator.json";

fn calc_part(node_cnt: u32, critical_path: u32) -> u32 {
    return node_cnt / critical_path;
}

fn process_pure_dags(tt_input_file: &str, sample_cnt: usize, output_dir: &str) {
    let pure_dags = PureDags::get_from_file(tt_input_file);
    println!("Real work just starts");

    {
        let output_dir = String::from(output_dir) + "/tasks";
        fs::remove_dir_all(&output_dir).unwrap();
        fs::create_dir(&output_dir).unwrap();
        let pure_dags = pure_dags.samples(sample_cnt);

        for (job_name, pure_dag) in pure_dags.iter() {
            if pure_dag.node_count() < 7 {
                continue;
            }
            let task_dag1 = <TaskDag as TaskDagFuncs>::from_pure_dag(&pure_dag);
            task_dag1.save_to_file(&format!("{}/{}.json", &output_dir, job_name).to_string());
            task_dag1.save_to_dot(&format!("{}/{}.dot", &output_dir, job_name).to_string());
        }
    }

    // Examples of calc stat
    let mut cp_ranges = CpStatistic::new();
    let mut level_distr_gen = StructStatistic::new();
    let mut level_gen = LevelGenerator::new();

    for (_, graph) in pure_dags.dags.iter() {
        let node_cnt = graph.node_count();
        let mut depths = vec![0; node_cnt];
        let mut levels = vec![0; node_cnt];

        graph.calc_levels(&mut depths, &mut levels);

        let critical_path = depths.iter().max().unwrap();
        // println!("{:?}", critical_path);
        cp_ranges.add(critical_path, node_cnt as u32);

        let part = calc_part(node_cnt as u32, *critical_path);
        level_distr_gen.add(*critical_path, part, &levels);

        // many massive statistics/ Yes, bad api again, but better
        let (result_time, result_cpu, result_cpu_diff_max) =
            graph.get_inst_inf(*critical_path as usize, &levels);

        level_gen.add_statistic(
            *critical_path,
            part,
            "instance_distr",
            graph,
            |graph: &PureDag| -> Vec<Vec<u32>> {
                let mut result = vec![Vec::new(); *critical_path as usize];
                for node_ind in graph.node_indices() {
                    let node_info = graph.node_weight(node_ind).unwrap();
                    result[levels[node_ind.index() as usize] as usize]
                        .push(node_info.instance_cnt as u32);
                }
                return result;
            },
        );

        level_gen.add_statistic(
            *critical_path,
            part,
            "time_distrib",
            graph,
            move |_: &PureDag| -> Vec<Vec<u32>> {
                return result_time;
            },
        );

        level_gen.add_statistic(
            *critical_path,
            part,
            "cpu_distribution",
            graph,
            move |_: &PureDag| -> Vec<Vec<u32>> {
                return result_cpu;
            },
        );

        level_gen.add_statistic(
            *critical_path,
            part,
            "diff_cpu_distribution",
            graph,
            move |_: &PureDag| -> Vec<Vec<u32>> {
                return result_cpu_diff_max;
            },
        );

        let (straight_links, union_links) = graph.get_links_per_type(&levels);

        level_gen.add_statistic(
            *critical_path,
            part,
            "straigt_links_cnts",
            graph,
            move |_: &PureDag| -> Vec<Vec<u32>> {
                let mut result: Vec<Vec<u32>> = vec![Vec::new(); straight_links.len()];
                for (val_result, val) in result.iter_mut().zip(straight_links.iter()) {
                    val_result.push(*val);
                }
                return result;
            },
        );
        level_gen.add_statistic(
            *critical_path,
            part,
            "union_links_cnts",
            graph,
            move |_: &PureDag| -> Vec<Vec<u32>> {
                let mut result: Vec<Vec<u32>> = vec![Vec::new(); union_links.len()];
                for (val_result, val) in result.iter_mut().zip(union_links.iter()) {
                    val_result.push(*val);
                }
                return result;
            },
        );
    }

    {
        let output_dir = String::from(output_dir) + "/stats";
        let stat_name = |filename: &str| -> String { format!("{}/{}", output_dir, filename) };
        cp_ranges.save_to_file(&stat_name(CP_RANGES_FILENAME).to_string());
        level_distr_gen.save_to_file(&stat_name(LEVEL_DISTRIB_FILENAME).to_string());
        level_gen.save_to_file(&stat_name(LEVEL_GENERAOTR_FILENAME).to_string());
    }
}

const MIN_CP: u32 = 5;
const MAX_CP: u32 = 7;

fn gen_task_graph(sample_cnt: usize, work_dir: &str) {
    // Examples of use stat
    let mut cp_gen_ranges = CpStatistic::new();
    let mut level_distr_gen = StructStatistic::new();
    let mut level_gen = LevelGenerator::new();
    {
        let work_dir = String::from(work_dir) + "/stats";
        let stat_name = |filename: &str| -> String { format!("{}/{}", work_dir, filename) };

        cp_gen_ranges.load_from_file(&stat_name(CP_RANGES_FILENAME).to_string());
        level_distr_gen.load_from_file(&stat_name(LEVEL_DISTRIB_FILENAME).to_string());
        level_gen.load_from_file(&stat_name(LEVEL_GENERAOTR_FILENAME).to_string());
    }

    let mut rnd = rand::thread_rng();
    for job_gen in 0..sample_cnt {
        let cp = rnd.gen_range(MIN_CP..MAX_CP) as u32;

        let node_cnt = cp_gen_ranges.get_node_cnt(&mut rnd, cp);
        println!("cp {}, node_cnt {:?}", cp, node_cnt);
        if node_cnt.is_none() {
            return;
        }
        let node_cnt = node_cnt.unwrap();
        let mut result_dag = TaskDag::new();

        let mut part = calc_part(node_cnt, cp);
        level_distr_gen.adjust_part(cp, &mut part);

        // asign levels
        let mut node_level: Vec<u32> = vec![0; node_cnt as usize];
        for i in 0..cp {
            *node_level.get_mut(i as usize).unwrap() = i;
        }
        for i in cp..node_cnt {
            let level = level_distr_gen.gen_level(&mut rnd, cp, part);
            match level {
                Some(level) => {
                    *node_level.get_mut(i as usize).unwrap() = level;
                }
                None => {
                    panic!("Something with {:?}", level);
                }
            }
        }

        // Add nodes to graph and hroup by level
        let mut levels_storage = vec![Vec::<NodeIndex>::new(); cp as usize];
        for i in 0..node_cnt {
            let node_lv = node_level[i as usize];
            // let flops_sz = // was something else and complex gen_instance_cnt(
            //     ;
            let instance_cnt =
                level_gen.get_statistic(cp, part, node_lv, "instance_distr", &mut rnd);

            let flops_sz = level_gen.get_statistic(cp, part, node_lv, "time_distrib", &mut rnd);

            levels_storage[node_lv as usize].push(result_dag.add_node(DagVertex {
                task_name: format!("task_{}", i),
                dependencies: Vec::new(),
                instance_cnt: instance_cnt.ceil() as u64,
                flops: flops_sz,
            }));
        }
        let levels_storage = levels_storage;

        let mut gen_rand_indexes = |level: usize, cnt: usize| -> Vec<NodeIndex> {
            let mut ind_renage: Vec<usize> = (0..levels_storage[level].len()).collect();
            ind_renage.shuffle(&mut rnd);
            return ind_renage[0..cnt]
                .iter()
                .map(|node_ind| levels_storage[level][*node_ind])
                .collect();
        };

        let mut rnd1 = rand::thread_rng();

        // assign edges
        for level in 0..cp {
            if level + 1 != cp {
                // make at least one successor
                for node_ind in levels_storage[level as usize].iter() {
                    let child_ind = gen_rand_indexes(level as usize + 1, 1)[0];
                    result_dag.add_task_endge(child_ind, *node_ind);
                }
            }
            if level != 0 {
                // and many predecesors
                let level = level as usize;
                let prev_level_len = levels_storage[level - 1].len();
                let average_edges =
                    (levels_storage[level].len() / prev_level_len).min(prev_level_len);
                let average_edges = (average_edges as i32 - rnd1.gen_range(0..2)).max(1) as usize;
                for node_ind in levels_storage[level].iter() {
                    let depend = result_dag
                        .node_weight(*node_ind)
                        .unwrap()
                        .dependencies
                        .clone();
                    if depend.len() == average_edges {
                        continue;
                    }
                    for parent_ind in gen_rand_indexes(level - 1, average_edges) {
                        // let parent_ind = find_index(level - 1);
                        if depend.contains(&(parent_ind.index() as u32)) {
                            continue;
                        }
                        result_dag.add_task_endge(*node_ind, parent_ind);
                    }
                }
            }
        }

        result_dag.save_to_file(&format!("{}/tasks/{}.json", work_dir, job_gen).to_string());
        result_dag.save_to_dot(&format!("{}/tasks/{}.dot", work_dir, job_gen).to_string());
    }
}

const CCR: f64 = 11.0; // comp / comm
use std::fs;

fn gen_inst(dirpath: &str) {
    let mut rnd = rand::thread_rng();
    let paths = fs::read_dir(format!("{}/tasks", dirpath)).unwrap();
    let mut result_dag = TaskDag::new();
    fs::remove_dir_all(format!("{}/inss/", dirpath)).unwrap();
    fs::create_dir(format!("{}/inss/", dirpath)).unwrap();
    for path in paths {
        let path = path.unwrap().file_name().into_string().unwrap();
        let filename: &str = path.split('.').collect::<Vec<&str>>()[0];
        println!("{}", filename);
        result_dag.load_from_file(&format!("{}/tasks/{}.json", dirpath, filename).to_string());
        let instance_dag = result_dag.convert_to_inst_dag(&mut rnd, CCR);
        instance_dag.save_to_dot(&format!("{}/inss/{}.dot", dirpath, filename).to_string());
    }
}
use std::env;

const INPUT_FILENAME: &str = "../dump_files/save_result_ins.json";

fn type_devided() {
    // read graphs data
    let jobs = PureDags::get_from_file(INPUT_FILENAME);

    // let mut statistic_to_draw = CpStatistic::new();
    println!("real work just start");

    let mut jobs_tree_increase = PureDags::new();
    let mut jobs_tree_decrease = PureDags::new();
    let mut jobs_tree_others = PureDags::new();

    let mut glocal_tree_cnts = 0;
    for (job_show, graph) in jobs.dags.into_iter() {
        // println!("{}", job_show);
        let node_cnt = graph.node_count();
        let mut depths = vec![0; node_cnt];
        let mut used = vec![0; node_cnt];

        // here only save critical path

        let mut is_tree = true;
        let mut roots_cnt = 0;
        let mut sinks_cnt = 0;
        for ind in graph.node_indices() {
            let depend_len = graph.node_weight(ind).unwrap().dependences.len();
            if depend_len == 0 {
                if graph.dfs(ind, &mut depths, &mut used, &mut is_tree) != 0 {
                    // println!("find cycle in graph for job {}", job_show);
                }
                roots_cnt += 1;
            }
            if graph.neighbors(ind).count() == 0 {
                sinks_cnt += 1;
            }
        }
        if is_tree {
            glocal_tree_cnts += 1;

            if roots_cnt >= sinks_cnt {
                jobs_tree_decrease.insert(job_show, graph);
            } else {
                jobs_tree_increase.insert(job_show, graph);
            }
        } else {
            jobs_tree_others.insert(job_show, graph);
        }
    }
    println!("tree is found at count: {}", glocal_tree_cnts);

    for (filename, jobs_container) in [
        ("tree_incr", jobs_tree_increase),
        ("tree_decr", jobs_tree_decrease),
        ("other", jobs_tree_others),
    ]
    .iter()
    {
        println!("{} has {} dags", filename, jobs_container.dags.len());
        jobs_container.save_to_file(filename);
    }
    // save for other steps
}

fn main() {
    let final_dir = "../tree_incr";
    let source_dir = "../by_graph_type/tree_incr.json";
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        match &args[1][..] {
            "form" => type_devided(),
            "pure" => process_pure_dags(source_dir, 1000, final_dir),
            "task" => gen_task_graph(100, final_dir),
            "ins" => gen_inst(final_dir),
            "help" => {
                println!("form -> pure -> task -> ins");
            }
            _ => {
                println!("check help");
            }
        };
    }
    println!("Ok");
}
