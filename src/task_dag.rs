use crate::instance::{AddEdge, InstDagVertex, InstanceDag};
use rand::rngs::ThreadRng;
use rand::Rng;

use crate::pure_dag::PureDag;
use petgraph::stable_graph::NodeIndex;

use petgraph::visit::EdgeRef;
use petgraph::{Directed, Graph};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

#[derive(Serialize, Deserialize)]
pub struct DagVertex {
    pub task_name: String,
    pub dependencies: Vec<u32>,
    pub instance_cnt: u64,
    pub flops: f64,
}

pub type TaskDag = Graph<DagVertex, u64, Directed>;

pub const MAX_INST_CNT: u64 = 20;

pub trait TaskDagFuncs {
    fn from_pure_dag(pure_dag: &PureDag) -> TaskDag;
    fn convert_to_inst_dag(&self, rnd: &mut ThreadRng, ccr: f64) -> InstanceDag;
    fn save_to_file(&self, file_name: &str);
    fn load_from_file(&mut self, file_name: &str);
    fn add_task_endge(&mut self, child_ind: NodeIndex, parent_ind: NodeIndex);
}

impl TaskDagFuncs for TaskDag {
    fn from_pure_dag(pure_dag: &PureDag) -> TaskDag {
        let mut result = TaskDag::new();

        let mut true_order = vec![NodeIndex::new(0); pure_dag.node_count()];

        let mut map_orders = HashMap::<NodeIndex, usize>::new();
        for node_ind in pure_dag.node_indices() {
            let node_info = pure_dag.node_weight(node_ind).unwrap();
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
            let node_info = pure_dag.node_weight(node_ind).unwrap();

            result.add_node(DagVertex {
                task_name: node_info.name.clone(),
                dependencies: node_info.dependences.clone(),
                instance_cnt: node_info.instance_cnt.min(MAX_INST_CNT),
                flops: (node_info.end_time - node_info.start_time) as f64,
            });
        }

        for edge in pure_dag.edge_references() {
            result.add_edge(
                NodeIndex::new(map_orders[&edge.source()]),
                NodeIndex::new(map_orders[&edge.target()]),
                *edge.weight(),
            );
        }
        return result;
    }

    fn convert_to_inst_dag(&self, rnd: &mut ThreadRng, ccr: f64) -> InstanceDag {
        let mut instance_dag = InstanceDag::new();
        let mut global_counter: usize = 0;
        let node_cnt = self.node_count();
        let mut start_task_index: Vec<usize> = vec![0; node_cnt];
        let mut vector_edge: Vec<f64> = vec![0.0; node_cnt];

        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            // gen instance with flops
            let flops_sz = node_info.flops;
            start_task_index[node_ind.index()] = global_counter;
            for i in 0..node_info.instance_cnt.min(40) {
                instance_dag.add_node(InstDagVertex {
                    inst_name: format!("{}_{}", node_info.task_name, i).to_string(),
                    dependencies: Vec::new(),
                    flops: flops_sz,
                });
                global_counter += 1;
            }
            vector_edge[node_ind.index()] = flops_sz / ccr * rnd.gen_range(0.9..1.1);
        }

        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            let mut parent_ins_sum = 0;
            for parent in node_info.dependencies.iter() {
                parent_ins_sum += self
                    .node_weight(NodeIndex::new(*parent as usize))
                    .unwrap()
                    .instance_cnt
                    .min(40);
            }

            if parent_ins_sum == node_info.instance_cnt.min(40) {
                // case union
                let mut node_ind_sl = start_task_index[node_ind.index()];

                for parent in node_info.dependencies.iter() {
                    let parent = NodeIndex::new(*parent as usize);
                    let parent_info = self.node_weight(parent).unwrap();
                    let mut parent_ind_sl = start_task_index[parent.index()];
                    for _ in 0..parent_info.instance_cnt.min(40) {
                        instance_dag.add_ins_edge(
                            parent_ind_sl,
                            node_ind_sl,
                            vector_edge[parent.index()],
                        );
                        (node_ind_sl, parent_ind_sl) = (node_ind_sl + 1, parent_ind_sl + 1);
                    }
                }
            } else {
                for parent in node_info.dependencies.iter() {
                    let parent = NodeIndex::new(*parent as usize);

                    let parent_info = self.node_weight(parent).unwrap();
                    let parent_edge_out = vector_edge[parent.index()];

                    if parent_info.instance_cnt.min(40) == node_info.instance_cnt.min(40) {
                        // case map, filter
                        let mut parent_ind_sl = start_task_index[parent.index()];
                        let mut node_ind_sl = start_task_index[node_ind.index()];

                        for _ in 0..parent_info.instance_cnt.min(40) {
                            instance_dag.add_ins_edge(parent_ind_sl, node_ind_sl, parent_edge_out);
                            (node_ind_sl, parent_ind_sl) = (node_ind_sl + 1, parent_ind_sl + 1);
                        }
                    } else {
                        // groupByKey
                        for parent_ind_sl in (0..parent_info.instance_cnt.min(40))
                            .map(|x| start_task_index[parent.index()] + x as usize)
                        {
                            for node_ind_sl in (0..node_info.instance_cnt.min(40))
                                .map(|x| start_task_index[node_ind.index()] + x as usize)
                            {
                                instance_dag.add_ins_edge(
                                    parent_ind_sl,
                                    node_ind_sl,
                                    parent_edge_out,
                                );
                            }
                        }
                    }
                }
            }
        }
        return instance_dag;
    }

    fn save_to_file(&self, file_name: &str) {
        let j = serde_json::to_string(&self).unwrap();

        let path = Path::new(file_name);
        let mut file = match File::create(&path) {
            Err(why) => panic!("cant open file to write {}", why),
            Ok(file) => file,
        };

        match file.write_all(j.as_bytes()) {
            Err(why) => panic!("cant save serialization {}", why),
            Ok(_) => {}
        }
    }

    fn load_from_file(&mut self, file_name: &str) {
        let path = Path::new(file_name);
        let mut file = match File::open(&path) {
            Err(why) => panic!("cant open file to write {}", why),
            Ok(file) => file,
        };
        let mut contents = String::new();
        match file.read_to_string(&mut contents) {
            Err(why) => panic!("couldn't read: {}", why),
            Ok(_) => {}
        }

        *self = serde_json::from_str(&contents).unwrap();
    }

    fn add_task_endge(self: &mut TaskDag, child_ind: NodeIndex, parent_ind: NodeIndex) {
        self.add_edge(parent_ind, child_ind, 1);
        self.node_weight_mut(child_ind)
            .unwrap()
            .dependencies
            .push(parent_ind.index() as u32);
    }
}

pub trait SaveToFormatStructured {
    fn save_to_dot(&self, filename: &str);
}
use colors_transform::Rgb;

impl SaveToFormatStructured for TaskDag {
    fn save_to_dot(&self, filename: &str) {
        let mut f = File::create(filename).unwrap();
        let mut output = format!("digraph {{\n",);

        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            let ins_cnt = node_info.instance_cnt as f32;
            let time_amnt = node_info.flops as f32;
            let heavy_score = 2.0 * ins_cnt * time_amnt / (ins_cnt + time_amnt);
            let heavy_score = heavy_score.log2();
            let heavy_red_score = heavy_score / 10.0 * 256.0;
            let hex_color =
                Rgb::from(heavy_red_score, 0.0, 256.0 - heavy_red_score).to_css_hex_string();

            output.push_str(
                format!(
                    "{} [size=\"{}\", color=\"{}\"];\n",
                    node_info.task_name, node_info.flops, hex_color
                )
                .as_str(),
            );

            for neighbour in self.neighbors(node_ind) {
                let edge = self.edges_connecting(node_ind, neighbour).last().unwrap();
                output.push_str(
                    format!(
                        "{} -> {} [size=\"{}\"];\n",
                        node_info.task_name,
                        self.node_weight(neighbour).unwrap().task_name,
                        edge.weight()
                    )
                    .as_str(),
                );
            }
        }
        output.push_str("}\n");
        f.write_all(&output.as_bytes()).unwrap();
    }
}

pub trait FeatureCount {
    fn sparity(&self) -> f64;
    fn in_degree(&self) -> Vec<u64>;
    fn out_degree(&self) -> Vec<u64>;
    fn chain_ratio(&self) -> f64;
    fn pairwise_ins_ration(&self) -> Vec<f64>;
    fn pairwise_flops_ration(&self) -> Vec<f64>;
}

impl FeatureCount for TaskDag {
    fn sparity(&self) -> f64 {
        let node_nct = self.node_count() as f64;
        let edge_cnt = self.edge_count() as f64;
        return 2.0 * edge_cnt / node_nct / (node_nct - 1.0);
    }
    fn in_degree(&self) -> Vec<u64> {
        let mut result = Vec::new();
        for weight in self.node_weights() {
            result.push(weight.dependencies.len() as u64);
        }
        return result;
    }
    fn out_degree(&self) -> Vec<u64> {
        let mut result = Vec::new();
        for node_id in self.node_indices() {
            result.push(self.neighbors(node_id).count() as u64);
        }
        return result;
    }
    fn chain_ratio(&self) -> f64 {
        let mut result: u32 = 0;
        for node_id in self.node_indices() {
            if self.neighbors(node_id).count() == 1
                && self.node_weight(node_id).unwrap().dependencies.len() == 1
            {
                result += 1;
            }
        }
        return result as f64 / self.node_count() as f64;
    }
    fn pairwise_ins_ration(&self) -> Vec<f64> {
        let mut result = Vec::<f64>::new();
        for node_id in self.node_indices() {
            let ins_this = self.node_weight(node_id).unwrap().instance_cnt;
            for neight in self.neighbors(node_id) {
                result
                    .push(ins_this as f64 / self.node_weight(neight).unwrap().instance_cnt as f64);
            }
        }
        return result;
    }

    fn pairwise_flops_ration(&self) -> Vec<f64> {
        let mut result = Vec::<f64>::new();
        for node_id in self.node_indices() {
            let ins_this = self.node_weight(node_id).unwrap().flops;
            for neight in self.neighbors(node_id) {
                let chile = self.node_weight(neight).unwrap().flops;
                if chile == 0.0 {
                    continue;
                }
                result.push(ins_this as f64 / chile);
            }
        }
        return result;
    }
}
