use petgraph::graph::Node;
use petgraph::stable_graph::NodeIndex;

use petgraph::{Directed, Graph};
use std::fs::File;
use std::io::Write;

pub struct InstDagVertex {
    pub inst_name: String,
    pub dependencies: Vec<NodeIndex>,
    pub flops: f64,
}

pub type InstanceDag = Graph<InstDagVertex, f64, Directed>;

pub trait AddEdge {
    fn add_ins_edge(&mut self, parent_ind_sl: usize, node_ind_sl: usize, weight: f64);
}

impl AddEdge for InstanceDag {
    fn add_ins_edge(&mut self, parent_ind_sl: usize, node_ind_sl: usize, weight: f64) {
        let parent = NodeIndex::new(parent_ind_sl);
        let ch = NodeIndex::new(node_ind_sl);
        self.add_edge(parent, ch, weight);
        self.node_weight_mut(ch).unwrap().dependencies.push(parent);
    }
}

pub trait SaveToFormat {
    fn save_to_dot(&self, filename: &str);
    fn save_to_yaml(&self, filename: &str);
    fn save_to_yaml_rev(&self, filename: &str);
}

impl SaveToFormat for InstanceDag {
    fn save_to_dot(&self, filename: &str) {
        let mut f = File::create(filename).unwrap();
        let mut output = format!("digraph {{\n",);

        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            output.push_str(
                format!("{} [size=\"{}\"];\n", node_info.inst_name, node_info.flops).as_str(),
            );

            for neighbour in self.neighbors(node_ind) {
                let edge = self.edges_connecting(node_ind, neighbour).last().unwrap();
                output.push_str(
                    format!(
                        "{} -> {} [size=\"{}\"];\n",
                        node_info.inst_name,
                        self.node_weight(neighbour).unwrap().inst_name,
                        edge.weight()
                    )
                    .as_str(),
                );
            }
        }
        output.push_str("}\n");
        f.write_all(&output.as_bytes()).unwrap();
    }
    fn save_to_yaml(&self, filename: &str) {
        let mut f = File::create(filename).unwrap();
        let mut output = format!("inputs:\n  - name: init\n    size: 0\ntasks:\n",);

        for node_ind in self.node_indices() {
            let node_info = self.node_weight(node_ind).unwrap();
            output.push_str(
                format!(
                    "  - name: {}\n    flops: {}\n    memory: 1\n",
                    node_info.inst_name,
                    (node_info.flops.ceil() as u64).max(1)
                )
                .as_str(),
            );
            let inputs = &node_info.dependencies;
            if inputs.len() != 0 {
                output.push_str("    inputs:\n");
                for input in inputs {
                    let input_info = self.node_weight(*input).unwrap();
                    output.push_str(
                        format!("      - {}_{}\n", input_info.inst_name, node_info.inst_name)
                            .as_str(),
                    );
                }
            } else {
                output.push_str("    inputs:\n      - init\n");
            }

            let outputs = self.neighbors(node_ind);
            if outputs.count() != 0 {
                output.push_str("    outputs:\n");
                for neighbour in self.neighbors(node_ind) {
                    let edge = self.edges_connecting(node_ind, neighbour).last().unwrap();
                    output.push_str(
                        format!(
                            "      - name: {}_{}\n        size: {}\n",
                            node_info.inst_name,
                            self.node_weight(neighbour).unwrap().inst_name,
                            edge.weight().max(1.0)
                        )
                        .as_str(),
                    );
                }
            } else {
                output.push_str("    outputs:\n      - name: result\n        size: 1\n")
            }
        }
        f.write_all(&output.as_bytes()).unwrap();
    }

    fn save_to_yaml_rev(&self, filename: &str) {
        let mut f = File::create(filename).unwrap();
        let mut output = format!("inputs:\n  - name: init\n    size: 0\ntasks:\n",);

        for node_ind in self.node_indices().rev() {
            let node_info = self.node_weight(node_ind).unwrap();
            output.push_str(
                format!(
                    "  - name: {}\n    flops: {}\n    memory: 1\n",
                    node_info.inst_name,
                    (node_info.flops.ceil() as u64).max(1)
                )
                .as_str(),
            );
            let inputs = self.neighbors(node_ind);
            if inputs.count() != 0 {
                output.push_str("    inputs:\n");
                for input in self.neighbors(node_ind) {
                    let input_info = self.node_weight(input).unwrap();
                    output.push_str(
                        format!("      - {}_{}\n", input_info.inst_name, node_info.inst_name)
                            .as_str(),
                    );
                }
            } else {
                output.push_str("    inputs:\n      - init\n");
            }

            let outputs = &node_info.dependencies;
            if outputs.len() != 0 {
                output.push_str("    outputs:\n");
                for neighbour in outputs {
                    let edge = self.edges_connecting(*neighbour, node_ind).last().unwrap();
                    output.push_str(
                        format!(
                            "      - name: {}_{}\n        size: {}\n",
                            node_info.inst_name,
                            self.node_weight(*neighbour).unwrap().inst_name,
                            edge.weight().max(1.0)
                        )
                        .as_str(),
                    );
                }
            } else {
                output.push_str("    outputs:\n      - name: result\n        size: 1\n")
            }
        }
        f.write_all(&output.as_bytes()).unwrap();
    }
}

pub trait Characters {
    fn get_all(&self) -> InstGrapgChar;
}

fn dfs_level(v: NodeIndex, dag: &InstanceDag, levels: &mut Vec<usize>) {
    for u in dag.neighbors(v) {
        if levels[u.index()] == 0 {
            levels[u.index()] = levels[v.index()] + 1;
            dfs_level(u, dag, levels);
        }
    }
}
fn dfs_cp(v: NodeIndex, dag: &InstanceDag, used: &mut Vec<f64>) -> f64 {
    let mut result = 0.0;

    for u in dag.neighbors(v) {
        if used[v.index()] != 0.0 {
            dfs_cp(u, dag, used);
        }
        let ne = used[v.index()];
        if ne > result {
            result = ne;
        }
    }
    return result + dag.node_weight(v).unwrap().flops;
}

extern crate itertools;

#[derive(Clone)]
pub struct InstGrapgChar {
    pub tasks_cnt: usize,
    pub depth: usize,
    pub width: usize,
    pub paralel: f64,
    pub max_work: f64,
    pub max_data: f64,
}

use itertools::Itertools;
impl Characters for InstanceDag {
    fn get_all(&self) -> InstGrapgChar {
        let tasks_cnt = self.node_count();
        let mut levels = vec![0; tasks_cnt];
        let mut used = vec![0.0; tasks_cnt];
        let mut cp_t = 0.0;
        for v in self.node_indices() {
            if self.node_weight(v).unwrap().dependencies.len() == 0 {
                dfs_level(v, self, &mut levels);
                let cp = dfs_cp(v, self, &mut used);
                if cp > cp_t {
                    cp_t = cp;
                }
            }
        }

        let depth = levels.iter().max().unwrap() + 1;
        levels.sort();
        let width = levels
            .iter()
            .group_by(|&x| x)
            .into_iter()
            .map(|(_, group)| group.count())
            .max()
            .unwrap();

        let parallelism = self.node_weights().map(|x| x.flops).sum::<f64>() / cp_t;
        let max_work = self
            .node_weights()
            .map(|x| x.flops)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let max_data = self
            .edge_weights()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        return InstGrapgChar {
            tasks_cnt: tasks_cnt,
            depth: depth,
            width: width,
            paralel: parallelism,
            max_work: max_work,
            max_data: *max_data,
        };
    }
}
