use std::collections::HashMap;

use dslab_compute::multicore::CoresDependency;

use crate::dag::DAG;

#[derive(Debug)]
struct Node {
    name: String,
    label: Option<String>,
    size: f64,
}

#[derive(Debug)]
struct Edge {
    from: String,
    to: String,
    label: Option<String>,
    size: f64,
}

fn parse_params(s: &[char]) -> HashMap<String, String> {
    let mut result = HashMap::new();
    for item in s.split(|&c| c == ',') {
        let mid = item.iter().position(|&c| c == '=');
        if mid.is_none() {
            continue;
        }
        let mid = mid.unwrap();
        let name = &item[0..mid];
        let value = &item[mid + 1..];
        if value.is_empty() || value[0] != '"' || value[value.len() - 1] != '"' {
            continue;
        }
        let value = &value[1..value.len() - 1];
        result.insert(name.iter().collect(), value.iter().collect());
    }
    result
}

impl DAG {
    /// Reads DAG from a file in [DOT format](https://graphviz.org/doc/info/lang.html).
    pub fn from_dot(file: &str) -> Self {
        let data = std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file));
        let lines = data.trim().split('\n').map(|x| x.to_string());

        let is_word_char = |c: char| -> bool { c.is_ascii_alphanumeric() || c == '_' };

        let mut nodes: Vec<Node> = Vec::new();
        let mut edges: Vec<Edge> = Vec::new();

        for line in lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let chars = line.chars().collect::<Vec<_>>();
            let mut i = 0;
            while i < chars.len() && is_word_char(chars[i]) {
                i += 1;
            }
            if i == 0 {
                continue;
            }
            let token1 = &chars[0..i];
            if token1.iter().collect::<String>() == "edge" || token1.iter().collect::<String>() == "node" {
                // reserved special name
                continue;
            }
            while i < chars.len() && chars[i].is_ascii_whitespace() {
                i += 1;
            }
            if i == chars.len() {
                continue;
            }
            if chars[i] == '-' {
                // edge
                // token1 -> token2 [label="1->2",size="1.5"]
                i += 1;
                if i == chars.len() || chars[i] != '>' {
                    continue;
                }
                i += 1;
                while i < chars.len() && chars[i].is_ascii_whitespace() {
                    i += 1;
                }
                let token_start = i;
                while i < chars.len() && is_word_char(chars[i]) {
                    i += 1;
                }
                if token_start == i {
                    continue;
                }
                let token2 = &chars[token_start..i];
                while i < chars.len() && chars[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i == chars.len() || chars[i] != '[' {
                    continue;
                }
                let mut r = chars.len() - 1;
                while r > i && chars[r] != ']' {
                    r -= 1;
                }
                if r <= i {
                    continue;
                }
                let mut params = parse_params(&chars[i + 1..r]);
                edges.push(Edge {
                    from: token1.iter().collect(),
                    to: token2.iter().collect(),
                    label: params.remove("label"),
                    size: params.get("size").unwrap().parse::<f64>().unwrap(),
                });
            } else {
                // node
                // token1 [label="node123",size="1.5"]
                while i < chars.len() && chars[i].is_ascii_whitespace() {
                    i += 1;
                }
                if i == chars.len() || chars[i] != '[' {
                    continue;
                }
                let mut r = chars.len() - 1;
                while r > i && chars[r] != ']' {
                    r -= 1;
                }
                if r <= i {
                    continue;
                }
                let mut params = parse_params(&chars[i + 1..r]);
                nodes.push(Node {
                    name: token1.iter().collect(),
                    label: params.remove("label"),
                    size: params.get("size").unwrap().parse::<f64>().unwrap(),
                });
            }
        }
        let mut dag = DAG::new();

        let mut task_ids: HashMap<String, usize> = HashMap::new();

        for node in nodes.into_iter() {
            task_ids.insert(
                node.name.clone(),
                dag.add_task(
                    &node.label.unwrap_or_else(|| node.name.clone()),
                    node.size.round() as u64,
                    0,
                    1,
                    1,
                    CoresDependency::Linear,
                ),
            );
        }

        let mut data_items: HashMap<String, usize> = HashMap::new();

        for edge in edges.into_iter() {
            let label = edge.label.unwrap_or(format!("{} -> {}", edge.from, edge.to));
            let from = *task_ids.get(&edge.from).unwrap();
            let to = *task_ids.get(&edge.to).unwrap();

            let data_item_id = *data_items
                .entry(label.clone())
                .or_insert_with(|| dag.add_task_output(from, &label, edge.size.round() as u64));
            dag.add_data_dependency(data_item_id, to);
        }

        dag
    }
}
