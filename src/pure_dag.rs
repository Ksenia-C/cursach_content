use petgraph::visit::EdgeRef;
use petgraph::{graph::Graph, stable_graph::NodeIndex, Directed};
// use std::collections::hash_map::IntoIter;

use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Read, io::Write, path::Path}; // 0.7.2

#[derive(Serialize, Deserialize, Clone)]
pub struct Instance {
    pub time: u64,
    pub cpu_avg: f64,
    pub cpu_diff_max: f64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct PureTaskInfo {
    pub name: String,
    pub instance_cnt: u64,
    pub start_time: u64,
    pub end_time: u64,
    pub dependences: Vec<u32>,
    pub instances: Vec<Instance>,
}

pub type PureDag = Graph<PureTaskInfo, u64, Directed>;

pub trait SortNodeIndex {
    fn sort_node_ids(&mut self);
}

impl SortNodeIndex for PureDag {
    fn sort_node_ids(&mut self) {
        let mut result = PureDag::new();

        let mut true_order = vec![NodeIndex::new(0); self.node_count()];

        let mut map_orders = HashMap::<NodeIndex, usize>::new();
        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            let task_name: &str = node_info.name.as_str();
            let number = &task_name[4..task_name.len()];
            let number = match number.parse::<usize>() {
                Ok(number) => number,
                Err(_) => 0,
            };

            true_order[number - 1] = node_ind;
            map_orders.insert(node_ind, number - 1);
        }
        for node_ind in true_order {
            let node_info = self.node_weight(node_ind).unwrap();

            let last_node = result.add_node(node_info.clone());
            result.node_weight_mut(last_node).unwrap().dependences =
                node_info.dependences.iter().map(|x| x - 1).collect();
        }

        for edge in self.edge_references() {
            result.add_edge(
                NodeIndex::new(map_orders[&edge.source()]),
                NodeIndex::new(map_orders[&edge.target()]),
                *edge.weight(),
            );
        }
        *self = result;
    }
}

pub struct PureDags {
    pub dags: HashMap<String, PureDag>,
}

impl PureDags {
    pub fn new() -> Self {
        return PureDags {
            dags: HashMap::new(),
        };
    }
    pub fn insert(&mut self, key: String, dag: PureDag) {
        self.dags.insert(key, dag);
    }

    pub fn get_from_file(filename: &str) -> Self {
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
        return PureDags {
            dags: serde_json::from_str(&contents).unwrap(),
        };
    }
    pub fn save_to_file(&self, filename: &str) {
        let j = serde_json::to_string(&self.dags).unwrap();

        let pull_file_path = format!("../by_graph_type/{}.json", filename);
        let path = Path::new(pull_file_path.as_str());
        let mut file = match File::create(&path) {
            Err(why) => panic!("cant open file to write {}", why),
            Ok(file) => file,
        };

        match file.write_all(j.as_bytes()) {
            Err(why) => panic!("cant save serialization {}", why),
            Ok(_) => {}
        };
    }

    pub fn samples(&self, n: usize) -> HashMap<String, PureDag> {
        let mut ranges: Vec<&String> = self.dags.keys().collect();
        let mut rnd = rand::thread_rng();
        ranges.shuffle(&mut rnd);
        let mut result: HashMap<String, PureDag> = HashMap::new();
        for key in ranges[..n].iter() {
            result.insert((*key).clone(), self.dags[*key].clone());
        }
        return result;
    }
}

pub enum Error {
    DFS(&'static str),
}

pub trait DoTraverse {
    fn dfs(
        &self,
        u: NodeIndex,
        depth: &mut Vec<u32>,
        used: &mut Vec<u32>,
        is_tree: &mut bool,
    ) -> i32;

    fn dfs_to_calc_first_level(
        &self,
        u: NodeIndex,
        visited_parents: &mut Vec<u32>,
        levels: &mut Vec<u32>,
    );

    fn dfs_to_calc_final_level(&self, u: NodeIndex, used: &mut Vec<u32>, levels: &mut Vec<u32>);
    fn calc_levels(&self, depths: &mut Vec<u32>, levels: &mut Vec<u32>) -> Option<Error>;
}

impl DoTraverse for PureDag {
    fn dfs(
        &self,
        u: NodeIndex,
        depth: &mut Vec<u32>,
        used: &mut Vec<u32>,
        is_tree: &mut bool,
    ) -> i32 {
        if used[u.index()] == 1 {
            *is_tree = false;
            return 1;
        }
        if used[u.index()] == 2 {
            *is_tree = false;
            return 0;
        }
        used[u.index()] = 1;
        let mut go_further = false;
        for v in self.neighbors(u) {
            go_further = true;

            // this condition must be legacy as I use dfs
            // if depth[v.index()] != 0 {
            //     continue;
            // }
            if self.dfs(v, depth, used, is_tree) != 0 {
                return 1;
            }
        }
        if !go_further {
            depth[u.index()] = 1;
        } else {
            for v in self.neighbors(u) {
                depth[u.index()] = depth[u.index()].max(depth[v.index()] + 1);
            }
        }
        used[u.index()] = 2;

        return 0;
    }

    fn dfs_to_calc_first_level(
        &self,
        u: NodeIndex,
        visited_parents: &mut Vec<u32>,
        levels: &mut Vec<u32>,
    ) {
        for v in self.neighbors(u) {
            visited_parents[v.index()] -= 1;
            // assign node level at least as his parent one
            levels[v.index()] = levels[v.index()].max(levels[u.index()] + 1);
            // when level is calculated fully go further to descendants
            if visited_parents[v.index()] == 0 {
                self.dfs_to_calc_first_level(v, visited_parents, levels);
            }
        }
    }

    fn dfs_to_calc_final_level(&self, u: NodeIndex, used: &mut Vec<u32>, levels: &mut Vec<u32>) {
        used[u.index()] = 1;
        let mut chl_cnt = 0;
        for v in self.neighbors(u) {
            if used[v.index()] == 0 {
                self.dfs_to_calc_final_level(v, used, levels);
            }
            chl_cnt += 1;
        }
        if self.node_weight(u).unwrap().dependences.len() < chl_cnt {
            let min_succ_level = self.neighbors(u).map(|ind| levels[ind.index()]).min();
            if min_succ_level.is_some() {
                levels[u.index()] = min_succ_level.unwrap() - 1;
            }
        }
    }

    fn calc_levels(&self, depths: &mut Vec<u32>, levels: &mut Vec<u32>) -> Option<Error> {
        let node_cnt = self.node_count();
        let mut used = vec![0; node_cnt];
        let mut dfs_used = vec![0; node_cnt];

        for ind in self.node_indices() {
            used[ind.index()] = self.node_weight(ind).unwrap().dependences.len() as u32;
        }
        for ind in self.node_indices() {
            let depend_len = self.node_weight(ind).unwrap().dependences.len();
            if depend_len == 0 {
                // calc depths
                let mut tmp = true;
                if self.dfs(ind, depths, &mut dfs_used, &mut tmp) != 0 {
                    return Some(Error::DFS("Found cycle in graph"));
                }
                // calculate lower bound of levels
                self.dfs_to_calc_first_level(ind, &mut used, levels)
            }
        }
        assert!(
            used.iter().sum::<u32>() == 0,
            "levels weren't calculated for all nodes"
        );
        let mut used = vec![0; node_cnt];
        for ind in self.node_indices() {
            let depend_len = self.node_weight(ind).unwrap().dependences.len();
            if depend_len == 0 {
                // calculate levels finnaly
                self.dfs_to_calc_final_level(ind, &mut used, levels)
            }
        }
        return None;
    }
}

pub trait AbsorbStat {
    fn get_inst_inf(
        &self,
        cp: usize,
        levels: &Vec<u32>,
    ) -> (Vec<Vec<u32>>, Vec<Vec<u32>>, Vec<Vec<u32>>);
    fn get_links_per_type(&self, levels: &Vec<u32>) -> (Vec<u32>, Vec<u32>);
}

impl AbsorbStat for PureDag {
    fn get_inst_inf(
        &self,
        cp: usize,
        levels: &Vec<u32>,
    ) -> (Vec<Vec<u32>>, Vec<Vec<u32>>, Vec<Vec<u32>>) {
        let mut result_time = vec![Vec::new(); cp];
        let mut result_cpu = vec![Vec::new(); cp];
        let mut result_cpu_diff_max = vec![Vec::new(); cp];

        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            for instance in node_info.instances.iter() {
                result_time[levels[node_ind.index() as usize] as usize].push(instance.time as u32);
                result_cpu[levels[node_ind.index() as usize] as usize]
                    .push(instance.cpu_avg as u32);
                result_cpu_diff_max[levels[node_ind.index() as usize] as usize]
                    .push(instance.cpu_diff_max as u32);
            }
        }
        return (result_time, result_cpu, result_cpu_diff_max);
    }

    fn get_links_per_type(&self, levels: &Vec<u32>) -> (Vec<u32>, Vec<u32>) {
        let mut straight_links: Vec<u32> = vec![0; levels.len()];
        let mut union_links: Vec<u32> = vec![0; levels.len()]; // REALLY CAN I use levels.len() instead of cp?

        let mut top_sort: Vec<usize> = (0..levels.len()).collect();
        top_sort.sort_by(|a, b| levels[*a].cmp(&levels[*b]));
        let mut prev_level = Vec::<usize>::new();
        let mut cur_level = prev_level.clone();
        let mut cur_level_ind = 0;
        let mut prev_level_sum = 0;
        for node in top_sort.iter() {
            if levels[*node] == cur_level_ind {
                cur_level.push(*node);
                if node != top_sort.last().unwrap() {
                    continue;
                }
            }
            for node in cur_level.iter() {
                let ins_cnt = self
                    .node_weight(NodeIndex::new(*node))
                    .unwrap()
                    .instances
                    .len();
                match prev_level.binary_search(&ins_cnt) {
                    Ok(_) => {
                        // inc straight links
                        *straight_links.get_mut(cur_level_ind as usize).unwrap() += 1;
                    }
                    Err(_) => {}
                }
                if ins_cnt == prev_level_sum {
                    // inc union links
                    *union_links.get_mut(cur_level_ind as usize).unwrap() += 1;
                }
            }
            cur_level_ind += 1;

            prev_level = cur_level;
            prev_level.sort();
            prev_level_sum = prev_level.iter().sum();
            cur_level = Vec::new();
            cur_level.push(*node);
        }
        (straight_links, union_links)
    }
}
