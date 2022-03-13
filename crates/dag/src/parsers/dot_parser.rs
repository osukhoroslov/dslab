use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;

use dot_parser::ast::*;

use compute::multicore::CoresDependency;

use crate::dag::*;

impl DAG {
    pub fn from_dot(file: &str) -> Self {
        let raw = std::fs::read_to_string(file).expect(&format!("Can't read file {}", file));
        let dot = Graph::read_dot(&raw).expect(&format!("Can't parse DOT from file {}", file));
        let mut dag = DAG::new();

        let mut task_ids: HashMap<String, usize> = HashMap::new();

        // first add all nodes
        for stmt in dot.stmts.stmts.iter() {
            match stmt {
                Stmt::NodeStmt(ns) => {
                    let name: String = ns.node.id.to_string();
                    let mut label = name.clone();
                    let mut size: f64 = 0.;

                    if let Some(attrs) = &ns.attr {
                        for alist in attrs.elems.iter() {
                            for &(field_name, field_value) in alist.elems.iter() {
                                // cut quotes
                                let field_value = &field_value[1..field_value.len() - 1];
                                if field_name == "label" {
                                    label = field_value.to_string();
                                } else if field_name == "size" {
                                    size = field_value.parse().unwrap();
                                }
                            }
                        }
                    }

                    task_ids.insert(
                        name.clone(),
                        dag.add_task(&label, size.round() as u64, 0, 1, 1, CoresDependency::Linear),
                    );
                }
                _ => {}
            }
        }

        let mut data_items: HashMap<String, usize> = HashMap::new();

        // then all edges
        for stmt in dot.stmts.stmts.iter() {
            match stmt {
                Stmt::EdgeStmt(es) => {
                    let from = *task_ids.get(es.node.id).unwrap();
                    let to = *task_ids.get(es.next.node.id).unwrap();
                    let mut label = format!("{} -> {}", es.node.id, es.next.node.id);
                    let mut size: f64 = 0.;

                    if let Some(attrs) = &es.attr {
                        for alist in attrs.elems.iter() {
                            for &(field_name, field_value) in alist.elems.iter() {
                                // cut quotes
                                let field_value = &field_value[1..field_value.len() - 1];
                                if field_name == "label" {
                                    label = field_value.to_string()
                                } else if field_name == "size" {
                                    size = field_value.parse().unwrap();
                                }
                            }
                        }
                    }

                    let entry = data_items.entry(label.clone());
                    let data_item_id: usize = match entry {
                        Occupied(x) => *x.get(),
                        Vacant(entry) => *entry.insert(dag.add_task_output(from, &label, size.round() as u64)),
                    };
                    dag.add_data_dependency(data_item_id, to);
                }
                _ => {}
            }
        }

        dag
    }
}
