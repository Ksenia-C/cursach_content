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
                            ((edge.weight() * 10.0).ceil() as u32).max(1)
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
