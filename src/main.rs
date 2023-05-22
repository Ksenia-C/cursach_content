use crate::get_gen_dag_stat::*;
use crate::instance::*;
use crate::pure_dag::*;
use crate::statistic::*;
use crate::task_dag::*;
use absorb::{main_instances, main_tasks, INS_INPUT_FILENAME};
use petgraph::stable_graph::NodeIndex;
use queues::*;
use rand::prelude::SliceRandom;
use std::collections::HashMap;
use std::fs;
use std::io::Write;

use crate::get_dag_stat::*;

use rand::rngs::ThreadRng;
use rand::Rng;
pub mod absorb;
pub mod get_dag_stat;
pub mod get_gen_dag_stat;
pub mod instance;
pub mod pure_dag;
pub mod statistic;
pub mod task_dag;

use env_logger::Builder;

// const TT_INPUT_FILENAME: &str = "../by_graph_type/tree_incr.json";
const CP_RANGES_FILENAME: &str = "cp_ranges.json";
const LEVEL_DISTRIB_FILENAME: &str = "level_distribute.json";
const LEVEL_GENERAOTR_FILENAME: &str = "level_generator.json";

fn calc_part(node_cnt: u32, critical_path: u32) -> u32 {
    return node_cnt / critical_path;
}

fn process_pure_dags(tt_input_dir: String, graph_type: &str, sample_cnt: usize, output_dir: &str) {
    // Examples of calc stat
    let mut cp_ranges = CpStatistic::new();
    let mut level_distr_gen = StructStatistic::new();
    let mut level_gen = LevelGenerator::new();

    let paths = fs::read_dir(tt_input_dir).unwrap();

    {
        let output_dir = String::from(output_dir) + "/tasks";
        if Path::new(&output_dir).exists() {
            fs::remove_dir_all(&output_dir).unwrap();
        }
        fs::create_dir(&output_dir).unwrap();
    }

    for path in paths {
        let mut dags_to_sample: HashMap<(u32, u32), Vec<String>> = [
            ((2, 4), Vec::new()),
            ((5, 7), Vec::new()),
            ((8, 10), Vec::new()),
            ((11, 14), Vec::new()),
            ((19, 24), Vec::new()),
        ]
        .iter()
        .cloned()
        .collect();
        let path = path.unwrap().path().display().to_string();
        if !path.contains(graph_type) {
            continue;
        }

        let mut pure_dags = PureDags::get_from_file(path.as_str());

        {
            let output_dir = String::from(output_dir) + "/tasks";
            if Path::new(&output_dir).exists() {
                fs::remove_dir_all(&output_dir).unwrap();
            }
            fs::create_dir(&output_dir).unwrap();
        }

        for (_job_name, graph) in pure_dags.dags.iter_mut() {
            graph.sort_node_ids();
            let node_cnt = graph.node_count();
            let mut depths = vec![0; node_cnt];
            let mut levels = vec![0; node_cnt];

            graph.calc_levels(&mut depths, &mut levels);

            let critical_path = depths.iter().max().unwrap();

            for (cp_range, sample_ex) in dags_to_sample.iter_mut() {
                if cp_range.0 <= *critical_path && *critical_path <= cp_range.1 {
                    sample_ex.push((*_job_name).clone());
                }
            }

            cp_ranges.add(critical_path, node_cnt as u32);

            let part = calc_part(node_cnt as u32, *critical_path);
            level_distr_gen.add(*critical_path, part, &levels);

            // many massive statistics/ Yes, bad api again, but better

            level_gen.add_statistic(
                *critical_path,
                part,
                "childs_distribution",
                graph,
                |graph: &PureDag| -> Vec<Vec<u32>> {
                    let mut result = vec![Vec::new(); *critical_path as usize];
                    for node_ind in graph.node_indices() {
                        let child_cnt = graph.neighbors(node_ind).count();
                        result[levels[node_ind.index() as usize] as usize].push(child_cnt as u32);
                    }
                    return result;
                },
            );
            level_gen.add_statistic(
                *critical_path,
                part,
                "dependances_distribution",
                graph,
                |graph: &PureDag| -> Vec<Vec<u32>> {
                    let mut result = vec![Vec::new(); *critical_path as usize];
                    for node_ind in graph.node_indices() {
                        let dep_cnt = graph.node_weight(node_ind).unwrap().dependences.len();
                        result[levels[node_ind.index() as usize] as usize].push(dep_cnt as u32);
                    }
                    return result;
                },
            );
            level_gen.add_statistic(
                *critical_path,
                part,
                "instance_distr_init",
                graph,
                |graph: &PureDag| -> Vec<Vec<u32>> {
                    let mut result = vec![Vec::new(); *critical_path as usize];
                    for node_ind in graph.node_indices() {
                        let node_info = graph.node_weight(node_ind).unwrap();
                        let node_level = levels[node_ind.index() as usize];
                        if node_info.dependences.len() == 0 {
                            result[node_level as usize]
                                .push(node_info.instance_cnt.min(MAX_INST_CNT) as u32);
                        }
                    }
                    return result;
                },
            );
            level_gen.add_statistic(
                *critical_path,
                part,
                "instance_distr_perc",
                graph,
                |graph: &PureDag| -> Vec<Vec<u32>> {
                    let mut result = vec![Vec::new(); *critical_path as usize];
                    for node_ind in graph.node_indices() {
                        let node_info = graph.node_weight(node_ind).unwrap();
                        let node_level = levels[node_ind.index() as usize];

                        if node_info.dependences.len() != 0 {
                            let mut depence_ins_avg = 0;
                            for parent in node_info.dependences.iter() {
                                depence_ins_avg += graph
                                    .node_weight(NodeIndex::new(*parent as usize))
                                    .unwrap()
                                    .instance_cnt
                                    .min(MAX_INST_CNT);
                            }
                            depence_ins_avg /= node_info.dependences.len() as u64;
                            result[node_level as usize].push(
                                (node_info.instance_cnt.min(MAX_INST_CNT) * 10000 / depence_ins_avg)
                                    as u32,
                            );
                        }
                    }
                    return result;
                },
            );

            let (result_time, _, _) = graph.get_inst_inf(*critical_path as usize, &levels);

            level_gen.add_statistic(
                *critical_path,
                part,
                "heavy_distr",
                graph,
                |graph: &PureDag| -> Vec<Vec<u32>> {
                    let mut result = vec![Vec::new(); *critical_path as usize];
                    for node_ind in graph.node_indices() {
                        let node_info = graph.node_weight(node_ind).unwrap();
                        let node_level = levels[node_ind.index() as usize];
                        let ins_cnt = node_info.instance_cnt.min(MAX_INST_CNT) as f64;
                        let time_amnt = (node_info.end_time - node_info.start_time) as f64;
                        let heavy_score = 2.0 * ins_cnt * time_amnt / (ins_cnt + time_amnt);
                        result[node_level as usize].push(heavy_score as u32);
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
        }
        let mut rnd = rand::thread_rng();
        for (cp_range, sample_ex) in dags_to_sample.iter_mut() {
            sample_ex.shuffle(&mut rnd);
            for key in sample_ex[..sample_cnt.min(sample_ex.len())].iter() {
                let pure_dag = pure_dags.dags[key].clone();

                let task_dag1 = <TaskDag as TaskDagFuncs>::from_pure_dag(&pure_dag);
                task_dag1.save_to_file(
                    &format!(
                        "{}/tasks/{}_{}_{}.json",
                        &output_dir, cp_range.0, cp_range.1, key
                    )
                    .to_string(),
                );
                task_dag1.save_to_dot(
                    &format!(
                        "{}/tasks/{}_{}_{}.dot",
                        &output_dir, cp_range.0, cp_range.1, key
                    )
                    .to_string(),
                );
            }
        }
        dags_to_sample.clear();
    }
    {
        let output_dir = String::from(output_dir) + "/stats";
        let stat_name = |filename: &str| -> String { format!("{}/{}", output_dir, filename) };
        cp_ranges.save_to_file(&stat_name(CP_RANGES_FILENAME).to_string());
        level_distr_gen.save_to_file(&stat_name(LEVEL_DISTRIB_FILENAME).to_string());
        level_gen.save_to_file(&stat_name(LEVEL_GENERAOTR_FILENAME).to_string());
    }
}

#[allow(dead_code)]
fn asign_edge_for_incr(
    node_cnt: u32,
    cp: u32,
    part: u32,
    node_level: &mut Vec<u32>,
    level_gen: &LevelGenerator,
    result_dag: &mut TaskDag,
    rnd: &mut ThreadRng,
    level_distr_gen: &StructStatistic,
) {
    let mut by_level = vec![Vec::<NodeIndex>::new(); cp as usize];
    for i in 0..cp as usize {
        by_level[i].push(NodeIndex::new(i));
    }
    let mut queue_to_assign = queue![NodeIndex::new(0)];
    let mut free_node_to_asign = node_cnt - cp;
    let mut last_level: i32 = 0;
    let mut last_node_ind = cp;
    while let Ok(cur_node_index) = queue_to_assign.remove() {
        let cur_ind = cur_node_index.index();
        let node_lv = node_level[cur_ind];
        if (node_lv as i32 != last_level || queue_to_assign.size() == 0)
            && last_level + 1 < cp as i32
        {
            last_level += 1;
            queue_to_assign
                .add(NodeIndex::new(last_level as usize))
                .unwrap();
        }
        let mut child_cnt = level_gen
            .get_statistic(cp, part, node_lv, "childs_distribution", rnd)
            .ceil() as usize;
        if cur_ind < cp as usize && child_cnt > 0 {
            child_cnt -= 1;
        }
        for _ in 0..child_cnt {
            free_node_to_asign -= 1;

            let child_node_ind = NodeIndex::new(last_node_ind as usize);
            last_node_ind += 1;
            result_dag.add_task_endge(child_node_ind, cur_node_index);
            queue_to_assign.add(child_node_ind).unwrap();

            node_level[child_node_ind.index()] = node_lv + 1;
            by_level[(node_lv + 1) as usize].push(child_node_ind);

            if free_node_to_asign == 0 {
                break;
            }
        }
        if free_node_to_asign == 0 {
            break;
        }
    }

    for _ in 0..free_node_to_asign as usize {
        node_level[last_node_ind as usize] =
            level_distr_gen.gen_level(rnd, cp, part).unwrap().max(1);
        let parent_level = node_level[last_node_ind as usize] - 1;
        let parent_ind = rnd.gen_range(0..(by_level[parent_level as usize].len()));
        result_dag.add_task_endge(
            NodeIndex::new(last_node_ind as usize),
            NodeIndex::new(parent_ind),
        );
        last_node_ind += 1;
    }
}

#[allow(dead_code)]
fn asign_edge_for_decr(
    node_cnt: u32,
    cp: u32,
    part: u32,
    node_level: &mut Vec<u32>,
    level_gen: &LevelGenerator,
    result_dag: &mut TaskDag,
    rnd: &mut ThreadRng,
    level_distr_gen: &StructStatistic,
) {
    let mut by_level = vec![Vec::<NodeIndex>::new(); cp as usize];
    for i in 0..cp as usize {
        by_level[i].push(NodeIndex::new(i));
    }

    let mut queue_to_assign = queue![NodeIndex::new((cp - 1) as usize)];
    let mut free_node_to_asign = node_cnt - cp;
    let mut last_level: i32 = cp as i32 - 1;
    let mut last_node_ind = cp;
    while let Ok(cur_node_index) = queue_to_assign.remove() {
        let cur_ind = cur_node_index.index();
        let node_lv = node_level[cur_ind];
        if (node_lv as i32 != last_level || queue_to_assign.size() == 0) && last_level > 0 {
            last_level -= 1;
            queue_to_assign
                .add(NodeIndex::new(last_level as usize))
                .unwrap();
        }
        let mut parent_cnt = level_gen
            .get_statistic(cp, part, node_lv, "dependances_distribution", rnd)
            .ceil() as usize;
        if cur_ind < cp as usize && parent_cnt > 0 {
            parent_cnt -= 1;
        }
        for _ in 0..parent_cnt {
            free_node_to_asign -= 1;

            let parent_node_ind = NodeIndex::new(last_node_ind as usize);
            by_level[(node_lv - 1) as usize].push(parent_node_ind);
            last_node_ind += 1;
            result_dag.add_task_endge(cur_node_index, parent_node_ind);
            queue_to_assign.add(parent_node_ind).unwrap();

            node_level[parent_node_ind.index()] = node_lv - 1;

            if free_node_to_asign == 0 {
                break;
            }
        }
        if free_node_to_asign == 0 {
            break;
        }
    }

    for _ in 0..free_node_to_asign as usize {
        node_level[last_node_ind as usize] = level_distr_gen
            .gen_level(rnd, cp, part)
            .unwrap()
            .min(cp - 2);
        let child_level = node_level[last_node_ind as usize] + 1;
        let child_ind = rnd.gen_range(0..(by_level[child_level as usize].len()));
        result_dag.add_task_endge(
            NodeIndex::new(child_ind),
            NodeIndex::new(last_node_ind as usize),
        );
        last_node_ind += 1;
    }
}

#[allow(dead_code)]
fn asign_edge_for_other(
    node_cnt: u32,
    cp: u32,
    part: u32,
    node_level: &mut Vec<u32>,
    level_gen: &LevelGenerator,
    result_dag: &mut TaskDag,
    rnd: &mut ThreadRng,
    level_distr_gen: &StructStatistic,
) {
    let mut by_level = vec![Vec::<NodeIndex>::new(); cp as usize];
    for i in 0..node_cnt {
        let cur_node_level = if i < cp {
            i as u32
        } else {
            level_distr_gen.gen_level(rnd, cp, part).unwrap()
        };

        node_level[i as usize] = cur_node_level;
        by_level[cur_node_level as usize].push(NodeIndex::new(i as usize));
    }
    for level in 0..(cp - 1) {
        for node in by_level[level as usize].iter() {
            let child_cnt = level_gen
                .get_statistic(cp, part, level, "childs_distribution", rnd)
                .ceil() as u32;
            let child_cnt = child_cnt.max(1);
            let mut next_level = by_level[(level + 1) as usize].clone();
            next_level.shuffle(rnd);
            for child_ind in 0..next_level.len().min(child_cnt as usize) {
                if node.index() < cp as usize && next_level[child_ind].index() < cp as usize {
                    continue;
                }

                result_dag.add_task_endge(next_level[child_ind], *node);
            }
        }
    }
    for node in by_level[cp as usize - 1].iter() {
        if result_dag.node_weight(*node).unwrap().dependencies.len() == 0 {
            let parent_ind = rnd.gen_range(0..by_level[cp as usize - 2].len());
            result_dag.add_task_endge(*node, by_level[cp as usize - 2][parent_ind]);
        }
    }
}

fn gen_task_graph(sample_cnt: usize, work_dir: &str, min_cp: u32, max_cp: u32) {
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
        let (cp, node_cnt) = loop {
            let cp = rnd.gen_range(min_cp..=max_cp) as u32;

            let node_cnt = cp_gen_ranges.get_node_cnt(&mut rnd, cp);
            if !node_cnt.is_none() {
                break (cp, node_cnt);
            }
        };
        let node_cnt = node_cnt.unwrap();
        let mut result_dag = TaskDag::new();

        let mut part = calc_part(node_cnt, cp);
        level_distr_gen.adjust_part(cp, &mut part);

        let mut node_level: Vec<u32> = vec![0; node_cnt as usize];
        for i in 0..cp {
            *node_level.get_mut(i as usize).unwrap() = i;
        }

        // Add empty nodes to graph
        for i in 0..node_cnt {
            result_dag.add_node(DagVertex {
                task_name: format!("task_{}", i),
                dependencies: Vec::new(),
                instance_cnt: 0,
                flops: 0.0,
            });
        }

        for i in 1..cp as usize {
            node_level[i] = i as u32;
            result_dag.add_task_endge(NodeIndex::new(i), NodeIndex::new(i - 1));
        }
        if work_dir.contains("incr") {
            asign_edge_for_incr(
                node_cnt,
                cp,
                part,
                &mut node_level,
                &level_gen,
                &mut result_dag,
                &mut rnd,
                &level_distr_gen,
            );
        } else if work_dir.contains("decr") {
            asign_edge_for_decr(
                node_cnt,
                cp,
                part,
                &mut node_level,
                &level_gen,
                &mut result_dag,
                &mut rnd,
                &level_distr_gen,
            );
        } else {
            asign_edge_for_other(
                node_cnt,
                cp,
                part,
                &mut node_level,
                &level_gen,
                &mut result_dag,
                &mut rnd,
                &level_distr_gen,
            );
        }

        for i in 0..node_cnt {
            let cur_node_ind = NodeIndex::new(i as usize);
            let node_lv = node_level[i as usize];
            let mut instance_cnt: f64;
            let parents_vector = &result_dag.node_weight(cur_node_ind).unwrap().dependencies;
            if parents_vector.len() != 0 {
                instance_cnt =
                    level_gen.get_statistic(cp, part, node_lv, "instance_distr_perc", &mut rnd);
                let mut avg_parent_ins: f64 = 0.0;
                for parent in parents_vector.iter() {
                    avg_parent_ins += result_dag
                        .node_weight(NodeIndex::new(*parent as usize))
                        .unwrap()
                        .instance_cnt as f64;
                }
                avg_parent_ins /= parents_vector.len() as f64;
                instance_cnt = avg_parent_ins as f64 * instance_cnt / 10000.0;
            } else {
                instance_cnt =
                    level_gen.get_statistic(cp, part, node_lv, "instance_distr_init", &mut rnd);
            }
            let instance_cnt = instance_cnt.ceil() as u64;

            let flops_sz = level_gen.get_statistic(cp, part, node_lv, "time_distrib", &mut rnd);

            let mut asign_weight = result_dag.node_weight_mut(cur_node_ind).unwrap();
            asign_weight.instance_cnt = instance_cnt.min(MAX_INST_CNT).max(1);
            asign_weight.flops = flops_sz;
        }

        result_dag.save_to_file(
            &format!("{}/tasks/{}_{}_{}.json", work_dir, min_cp, max_cp, job_gen).to_string(),
        );
        result_dag.save_to_dot(
            &format!("{}/tasks/{}_{}_{}.dot", work_dir, min_cp, max_cp, job_gen).to_string(),
        );
    }
}

fn gen_inst(dirpath: &str, ccr_use: f64) {
    let mut rnd = rand::thread_rng();
    let paths = fs::read_dir(format!("{}/tasks", dirpath)).unwrap();
    let mut result_dag = TaskDag::new();

    let inst_dir = format!("{}/inss_rev/", dirpath);
    if ccr_use < 0.9 {
        if Path::new(&inst_dir).exists() {
            fs::remove_dir_all(&inst_dir).unwrap();
        }
        fs::create_dir(&inst_dir).unwrap();
    }

    for path in paths {
        let path = path.unwrap().file_name().into_string().unwrap();
        let file_part = path.split('.').collect::<Vec<&str>>();
        let filename: &str = file_part[0];
        let file_ext = file_part[1];
        if file_ext != "json" {
            continue;
        }
        result_dag.load_from_file(&format!("{}/tasks/{}.json", dirpath, filename).to_string());
        let instance_dag = result_dag.convert_to_inst_dag(&mut rnd, ccr_use);
        instance_dag.save_to_dot(
            &format!("{}/inss_rev/{}_{}.dot", dirpath, filename, ccr_use * 10.0).to_string(),
        );
        instance_dag.save_to_yaml(
            &format!("{}/inss_rev/{}_{}.yaml", dirpath, filename, ccr_use * 10.0).to_string(),
        );
        instance_dag.save_to_yaml_rev(
            &format!(
                "{}/inss_rev/{}_{}.rev.yaml",
                dirpath,
                filename,
                ccr_use * 10.0
            )
            .to_string(),
        );
    }
}

use std::path::Path;

fn type_devided(k_part: usize) {
    // read graphs data
    let jobs = PureDags::get_from_file(INS_INPUT_FILENAME);

    let mut jobs_tree_increase = PureDags::new();
    let mut jobs_tree_decrease = PureDags::new();
    let mut jobs_tree_others = PureDags::new();

    let mut glocal_tree_cnts = 0;
    for (job_show, graph) in jobs.dags.into_iter() {
        let node_cnt = graph.node_count();
        let mut depths = vec![0; node_cnt];
        let mut used = vec![0; node_cnt];

        // here only save critical path

        let mut is_tree = true;
        let mut is_rev_tree = true;
        let mut is_chain = true;

        for ind in graph.node_indices() {
            let depend_len = graph.node_weight(ind).unwrap().dependences.len();
            if depend_len == 0 {
                if graph.dfs(ind, &mut depths, &mut used, &mut is_tree) != 0 {}
            } else if depend_len > 1 {
                is_chain = false;
            }
            match graph.neighbors(ind).count() {
                0..=1 => {}
                _ => {
                    is_rev_tree = false;
                    is_chain = false;
                }
            }
        }
        if is_chain {
            continue;
        }

        if is_tree {
            glocal_tree_cnts += 1;
            jobs_tree_increase.insert(job_show, graph);
        } else if is_rev_tree {
            glocal_tree_cnts += 1;
            jobs_tree_decrease.insert(job_show, graph);
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
        println!(
            "{}: {} has {} dags",
            k_part,
            filename,
            jobs_container.dags.len()
        );
        jobs_container.save_to_file(format!("{}{}", filename, k_part).as_str());
    }
}

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(about, long_about = None)]
struct Args {
    /// Take resouces from this directory
    #[clap(long, default_value = "help")]
    action: String,

    /// Take graphs from this directory
    #[clap(long, default_value = "other")]
    graph_type: String,

    /// Dump results into this directory
    #[clap(long, default_value_t = 5)]
    min_cp: u32,

    /// Dump results into this directory
    #[clap(long, default_value_t = 7)]
    max_cp: u32,

    /// Dump results into this directory
    #[clap(long, default_value_t = 0)]
    k_part: usize,

    /// Dump results into this directory
    #[clap(long, default_value_t = 10.0)]
    ccr_set: f64,

    /// Dump results into this directory
    #[clap(long, default_value = "unknown")]
    stat_task_name: String,
}

fn main() {
    Builder::from_default_env()
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();
    let args = Args::parse();

    let grapg_type: &str = &args.graph_type;

    let source_dir = String::from("../by_graph_type/");
    let final_dir = format!("../{}", grapg_type);
    match args.action.as_str() {
        "from_csv" => {
            main_tasks();
            println!("Ok main tasks");
            main_instances();
            println!("Ok main instances");
        }
        "form" => type_devided(args.k_part),
        "pure" => process_pure_dags(source_dir, grapg_type, 40, final_dir.as_str()),
        "task" => gen_task_graph(100, final_dir.as_str(), args.min_cp, args.max_cp),
        "ins" => gen_inst(final_dir.as_str(), args.ccr_set),
        "alib_art" => {
            stat_pure_dags(
                source_dir,
                format!("./st/pures_{}", args.stat_task_name).as_str(),
            );
            stat_task_dags(
                String::from("../tree_incr/tasks"),
                format!("./st/incr_{}", args.stat_task_name).as_str(),
            );
            stat_task_dags(
                String::from("../tree_decr/tasks"),
                format!("./st/decr_{}", args.stat_task_name).as_str(),
            );
            stat_task_dags(
                String::from("../other/tasks"),
                format!("./st/other_{}", args.stat_task_name).as_str(),
            );
        }
        _ => {
            println!("from_csv -> form -> pure -> task -> ins \n tree_incr tree_decr other");
        }
    };

    println!("Ok");
}
