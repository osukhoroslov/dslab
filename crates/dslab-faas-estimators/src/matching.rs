/// Utils for dealing with bipartite matching.

use std::collections::{HashSet, VecDeque};

/// Computes maximum cardinality matching in a convex bipartite graph with equal-length parts.
/// Assumes that for each node on the left its neighbors form an interval.
/// Result is a vector that contains the matched node for each node on the left
/// or usize::MAX if the node is unmatched.
pub fn convex_matching(n: usize, edges: Vec<(usize, usize)>) -> Vec<usize> {
    let mut min = vec![n; n];
    let mut max = vec![0; n];
    let mut cnt = vec![0; n];
    let mut right = vec![Vec::<usize>::new(); n];
    for (u, v) in edges.iter().copied() {
        min[u] = min[u].min(v);
        max[u] = max[u].max(v);
        cnt[u] += 1;
        right[v].push(u);
    }
    // convexity check
    for i in 0..n {
        assert!(cnt[i] == 0 || max[i] + 1 - min[i] == cnt[i]);
    }
    let mut mat = vec![usize::MAX; n];
    for i in 0..n {
        let mut chosen = usize::MAX;
        for j in right[i].drain(..) {
            if mat[j] == usize::MAX && (chosen == usize::MAX || max[j] < max[chosen]) {
                chosen = j;
            }
        }
        if chosen != usize::MAX {
            mat[chosen] = i;
        }
    }
    mat
}

#[derive(PartialEq, Eq, Copy, Clone)]
pub enum MatchingEdge {
    /// The edge is not in any maximal matching.
    InNone,
    /// The edge is in some maximal matchings, but not in all of them.
    InSome,
    /// The edge is in all maximal matchings.
    InAll,
}

/// Classifies graph edges with respect to the maximal matchings.
/// Requires any maximal matching (in format returned by [`convex_matching`]).
pub fn classify_edges(n: usize, edges: Vec<(usize, usize)>, matching: Vec<usize>) -> Vec<MatchingEdge> {
    let mut right = vec![Vec::new(); n];
    let mut left = vec![Vec::new(); n];
    let mut inv_match = vec![usize::MAX; n];
    for (u, v) in edges.iter().copied() {
        if matching[u] != v {
            right[v].push(u);
            left[u].push(v);
        } else {
            inv_match[v] = u;
        }
    }
    let mut stack = Vec::with_capacity(n * 2);
    let mut counter = 0;
    let mut order = Vec::with_capacity(n * 2);
    let mut used = vec![false; n * 2];
    let mut comp = vec![usize::MAX; n * 2];
    for i in 0..(2*n) {
        if used[i] {
            continue;
        }
        stack.push((i, false));
        while let Some(item) = stack.last_mut() {
            if item.1 == true {
                order.push(item.0);
                stack.pop();
                continue;
            }
            if used[item.0] {
                stack.pop();
                continue;
            }
            used[item.0] = true;
            item.1 = true;
            let v = item.0;
            if v < n && matching[v] != usize::MAX {
                let u = n + matching[v];
                if !used[u] {
                    stack.push((u, false));
                }
            } else if v >= n {
                for u in right[v - n].iter().copied() {
                    if !used[u] {
                        stack.push((u, false));
                    }
                }
            }
        }
    }
    assert_eq!(order.len(), 2 * n);
    order.reverse();
    for i in order.drain(..) {
        if comp[i] != usize::MAX {
            continue;
        }
        let curr_comp = counter;
        counter += 1;
        let mut q = VecDeque::new();
        q.push_back(i);
        comp[i] = curr_comp;
        while let Some(v) = q.pop_front() {
            if v < n {
                for u in left[v].iter().cloned() {
                    if comp[u] == usize::MAX {
                        q.push_back(u);
                        comp[u] = curr_comp;
                    }
                }
            } else {
                let u = inv_match[v - n];
                if u < usize::MAX && comp[u] == usize::MAX {
                    q.push_back(u);
                    comp[u] = curr_comp;
                }
            }
        }
    }
    let mut class = vec![MatchingEdge::InNone; edges.len()];
    for (i, (u, v)) in edges.iter().copied().enumerate() {
        if comp[u] == comp[v + n] {
            class[i] = MatchingEdge::InSome;
        }
    }
    used.fill(false);
    let mut found = HashSet::<(usize, usize)>::new();
    let mut q = VecDeque::new();
    for v in 0..n {
        if inv_match[v] == usize::MAX {
            q.push_back(v);
            used[v + n] = true;
        }
    }
    while let Some(v) = q.pop_front() {
        for u in right[v].iter().copied() {
            if !used[u] {
                let w = matching[u];
                assert!(w != usize::MAX);
                assert!(!used[w + n]);
                found.insert((u, v));
                found.insert((u, w));
                used[u] = true;
                used[w + n] = true;
                q.push_back(w);
            }
        }
    }
    for (i, edge) in edges.iter().enumerate() {
        if found.contains(edge) {
            class[i] = MatchingEdge::InSome;
        }
    }
    for (i, (u, v)) in edges.iter().copied().enumerate() {
        if class[i] == MatchingEdge::InNone && matching[u] == v {
            class[i] = MatchingEdge::InAll;
        }
    }
    class
}
