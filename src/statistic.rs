use crate::pure_dag::PureDag;
use rand::distributions::{Distribution, Uniform};

use inc_stats;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub trait StatBase {
    fn form_stats(&mut self);
    fn get_string_obj(&self) -> String;
    fn load_obj_from_string(&mut self, str: String);
}

pub trait SaveToFile: StatBase {
    fn save_to_file(&mut self, file_name: &str) {
        self.form_stats();
        let j = self.get_string_obj();

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

        self.load_obj_from_string(contents);
    }
}

pub struct CpStatistic {
    // stat: cp -> nodes_cnt
    stat: HashMap<u32, inc_stats::Percentiles<f64>>,
    stat_result: HashMap<u32, (f64, f64)>,
}

impl SaveToFile for CpStatistic {}

impl CpStatistic {
    pub fn new() -> Self {
        return CpStatistic {
            stat: HashMap::new(),
            stat_result: HashMap::new(),
        };
    }
    pub fn add(&mut self, critical_path: &u32, node_cnt: u32) {
        if !self.stat.contains_key(critical_path) {
            self.stat
                .insert(*critical_path, inc_stats::Percentiles::new());
        }
        let stats = self.stat.get_mut(critical_path).unwrap();

        stats.add(node_cnt as f64);
    }

    pub fn get_node_cnt(&self, rnd: &mut ThreadRng, cp: u32) -> Option<u32> {
        let cp_range = match self.stat_result.get(&cp) {
            Some(par) => par,
            None => {
                return None;
            }
        };
        return Some(rnd.gen_range(cp_range.0..=cp_range.1).ceil() as u32);
    }
}

impl StatBase for CpStatistic {
    fn form_stats(&mut self) {
        for (cp, percentiles) in self.stat.iter() {
            self.stat_result.insert(*cp, (0.0, 0.0));
            let range = percentiles.percentiles(&[0.2, 0.8]).unwrap().unwrap();
            self.stat_result.get_mut(cp).unwrap().0 = range[0];
            self.stat_result.get_mut(cp).unwrap().1 = range[1];
        }
    }
    fn get_string_obj(&self) -> String {
        return serde_json::to_string(&self.stat_result).unwrap();
    }
    fn load_obj_from_string(&mut self, str: String) {
        self.stat_result = serde_json::from_str(&str).unwrap();
    }
}

pub struct StructStatistic {
    // stat: cp, part -> level_distribution
    stat: HashMap<u32, HashMap<u32, Vec<u32>>>,
    stat_result: HashMap<u32, HashMap<u32, Vec<u32>>>,
}

impl SaveToFile for StructStatistic {}

impl StatBase for StructStatistic {
    fn form_stats(&mut self) {
        self.stat_result.clear();
        self.stat_result = self.stat.clone();
        for (_, part_info) in self.stat_result.iter_mut() {
            for (_, level_info) in part_info.iter_mut() {
                let prev = level_info.first();
                if prev.is_none() {
                    continue;
                }
                let mut prev = *prev.unwrap();
                for x in level_info.iter_mut().skip(1) {
                    *x += prev;
                    prev = *x;
                }
            }
        }
    }
    fn get_string_obj(&self) -> String {
        return serde_json::to_string(&self.stat_result).unwrap();
    }
    fn load_obj_from_string(&mut self, str: String) {
        self.stat_result = serde_json::from_str(&str).unwrap();
    }
}

impl StructStatistic {
    pub fn new() -> Self {
        return StructStatistic {
            stat: HashMap::new(),
            stat_result: HashMap::new(),
        };
    }

    pub fn add(&mut self, cp: u32, part: u32, levels: &Vec<u32>) {
        if !self.stat.contains_key(&cp) {
            self.stat.insert(cp, HashMap::new());
        }
        let cp_values = self.stat.get_mut(&cp).unwrap();
        if !cp_values.contains_key(&part) {
            cp_values.insert(part, vec![0; cp as usize]);
        }
        let part_values = cp_values.get_mut(&part).unwrap();
        for level in levels.iter() {
            part_values[*level as usize] += 1;
        }
    }

    pub fn gen_level(&self, rnd: &mut ThreadRng, cp: u32, part: u32) -> Option<u32> {
        let cumulative = match self.stat_result.get(&cp) {
            Some(ref par) => match par.get(&part) {
                Some(cum_distr) => cum_distr,
                None => {
                    return None;
                }
            },
            None => {
                return None;
            }
        };

        let uniform_rnd = Uniform::from(0..*(cumulative.last()).unwrap());
        // all other assign accourding to distribution
        let tmp = uniform_rnd.sample(rnd);
        let level = match cumulative.binary_search(&tmp) {
            Result::Err(ind) => ind,
            Result::Ok(ind) => ind,
        };
        return Some(level as u32);
    }

    pub fn adjust_part(&self, cp: u32, part: &mut u32) {
        match self.stat_result.get(&cp) {
            Some(ref par) => {
                let mut keys = par.keys().map(|&x| x).collect::<Vec<u32>>();
                keys.sort();
                match keys.binary_search(part) {
                    Ok(_) => {}
                    Err(ind) => {
                        *part = keys[ind];
                    }
                }
            }
            None => {
                panic!("smh get cp with empty stat: {}", cp);
            }
        };
    }
}

pub type MetricSerial = inc_stats::Percentiles<f64>;

type StatSeries = HashMap<u32, HashMap<u32, Vec<MetricSerial>>>;
type StatSaved = HashMap<u32, HashMap<u32, Vec<Vec<f64>>>>;

trait MultiStatIndexes<T> {
    fn get_mut_by(&mut self, cp: u32, part: u32) -> &mut Vec<T>;
    fn get_by(&self, cp: u32, part: u32) -> &Vec<T>;
}

impl MultiStatIndexes<MetricSerial> for StatSeries {
    fn get_mut_by(&mut self, cp: u32, part: u32) -> &mut Vec<MetricSerial> {
        if !self.contains_key(&cp) {
            self.insert(cp, HashMap::new());
        }
        let cp_values = self.get_mut(&cp).unwrap();
        if !cp_values.contains_key(&part) {
            cp_values.insert(part, Vec::new());
        }
        let part_values = cp_values.get_mut(&part).unwrap();
        part_values.resize_with(cp as usize, || MetricSerial::new());
        return part_values;
    }
    fn get_by(&self, cp: u32, part: u32) -> &Vec<MetricSerial> {
        let cp_values = self.get(&cp).unwrap();
        let part_values = cp_values.get(&part).unwrap();
        return part_values;
    }
}
impl MultiStatIndexes<Vec<f64>> for StatSaved {
    fn get_mut_by(&mut self, cp: u32, part: u32) -> &mut Vec<Vec<f64>> {
        if !self.contains_key(&cp) {
            self.insert(cp, HashMap::new());
        }
        let cp_values = self.get_mut(&cp).unwrap();
        if !cp_values.contains_key(&part) {
            cp_values.insert(part, Vec::new());
        }
        let part_values = cp_values.get_mut(&part).unwrap();
        part_values.resize_with(cp as usize, || Vec::<f64>::new());
        return part_values;
    }
    fn get_by(&self, cp: u32, part: u32) -> &Vec<Vec<f64>> {
        let cp_values = self.get(&cp).unwrap();
        let part_values = cp_values.get(&part).unwrap();
        return part_values;
    }
}

pub struct LevelGenerator {
    // cp, part -> [level] -> some statistic to calc Percentiles
    stat: HashMap<String, StatSeries>,
    stat_result: HashMap<String, StatSaved>,
}

impl LevelGenerator {
    // want registry stat name with function and then pass graph
    pub fn new() -> Self {
        return LevelGenerator {
            stat: HashMap::new(),
            stat_result: HashMap::new(),
        };
    }
    pub fn add_statistic<F>(&mut self, cp: u32, part: u32, name: &str, graph: &PureDag, stat_gen: F)
    where
        F: FnOnce(&PureDag) -> Vec<Vec<u32>>,
    {
        if !self.stat.contains_key(name) {
            self.stat.insert(name.to_string(), StatSeries::new());
        }
        let upd_values = self.stat.get_mut(name).unwrap().get_mut_by(cp, part);
        let stat_values = stat_gen(&graph);
        for (upd_val, level_val) in upd_values.iter_mut().zip(stat_values.iter()) {
            for val in level_val {
                upd_val.add(*val as f64);
            }
        }
    }

    pub fn get_statistic(
        &self,
        cp: u32,
        part: u32,
        level: u32,
        name: &str,
        rnd: &mut ThreadRng,
    ) -> f64 {
        let upd_values = self
            .stat_result
            .get(name)
            .unwrap()
            .get_by(cp, part)
            .get(level as usize)
            .unwrap();
        let upd_values = &upd_values[1..=3];
        let ps = BASIC_PERCENTILES.map(|x| x * 10.0);
        let rnd_n = rnd.gen_range(0.0..10.0);
        if rnd_n < ps[0] {
            return upd_values[0];
        }
        if rnd_n <= ps[1] {
            return upd_values[0]
                + (upd_values[1] - upd_values[0]) / (ps[1] - ps[0]) * (rnd_n - ps[0]);
        }
        if rnd_n < ps[2] {
            return upd_values[1]
                + (upd_values[2] - upd_values[1]) / (ps[2] - ps[1]) * (rnd_n - ps[1]);
        }
        return upd_values[2];
    }
}

const BASIC_PERCENTILES: [f64; 5] = [0.0, 0.2, 0.4, 0.8, 1.0];

impl StatBase for LevelGenerator {
    fn form_stats(&mut self) {
        self.stat_result.clear();
        for (name, stat_values) in self.stat.iter() {
            // println!("{}", name);
            let mut result_cp = HashMap::new();
            for (cp, cp_values) in stat_values.iter() {
                let mut result_part = HashMap::new();
                for (part, part_values) in cp_values.iter() {
                    let mut result_level = Vec::new();
                    for level_stat in part_values.iter() {
                        let result_stat = if level_stat.count() == 0 {
                            vec![0.0; 5]
                        } else {
                            level_stat
                                .percentiles(&BASIC_PERCENTILES)
                                .unwrap()
                                .unwrap()
                                .iter()
                                .map(|&x| x)
                                .collect::<Vec<f64>>()
                        };
                        result_level.push(result_stat);
                    }
                    result_part.insert(*part, result_level);
                }
                result_cp.insert(*cp, result_part);
            }
            self.stat_result.insert((*name).clone(), result_cp);
        }
    }

    fn get_string_obj(&self) -> String {
        return serde_json::to_string(&self.stat_result).unwrap();
    }
    fn load_obj_from_string(&mut self, str: String) {
        self.stat_result = serde_json::from_str(&str).unwrap();
    }
}

impl SaveToFile for LevelGenerator {}
