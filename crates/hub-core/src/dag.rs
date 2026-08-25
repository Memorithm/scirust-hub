//! A minimal but correct directed acyclic graph.
//!
//! Ships now because workflow composition is on the Hub's roadmap and cycle
//! detection must be right from the start. Multi-node *orchestration* is not
//! implemented yet; this type is the foundation it will build on.

use std::collections::{BTreeMap, BTreeSet};

use crate::error::CoreError;

/// Structural limits for one DAG.
#[derive(Clone, Copy, Debug)]
pub struct DagLimits {
    pub max_nodes: usize,
    pub max_edges: usize,
}

impl Default for DagLimits {
    fn default() -> Self {
        Self {
            max_nodes: 1024,
            max_edges: 4096,
        }
    }
}

/// Generic DAG over node payloads `T`. Nodes are addressed by caller-chosen
/// string keys; iteration order is deterministic (BTreeMap ordering).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Dag<T> {
    nodes: BTreeMap<String, T>,
    /// edges[u] = set of v such that u depends on v (u -> v).
    edges: BTreeMap<String, BTreeSet<String>>,
}

impl<T> Default for Dag<T> {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
        }
    }
}

impl<T> Dag<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a node; re-adding an existing key replaces its payload.
    ///
    /// # Errors
    /// [`CoreError::Validation`] when exceeding `max_nodes`.
    pub fn add_node(
        &mut self,
        key: impl Into<String>,
        payload: T,
        limits: &DagLimits,
    ) -> Result<(), CoreError> {
        let key = key.into();
        if !self.nodes.contains_key(&key) && self.nodes.len() >= limits.max_nodes {
            return Err(CoreError::Validation(format!(
                "DAG cannot exceed {} nodes",
                limits.max_nodes
            )));
        }
        self.nodes.insert(key, payload);
        Ok(())
    }

    /// Declares that `from` must run after `to` (edge `from -> to`).
    ///
    /// # Errors
    /// [`CoreError::Validation`] when a node is missing, the edge limit is
    /// hit, or the edge would create a cycle (checked before insertion).
    pub fn add_edge(
        &mut self,
        from: &str,
        to: &str,
        limits: &DagLimits,
    ) -> Result<(), CoreError> {
        if !self.nodes.contains_key(from) || !self.nodes.contains_key(to) {
            return Err(CoreError::Validation(format!(
                "cannot connect unknown nodes {from:?} -> {to:?}"
            )));
        }
        if from == to {
            return Err(CoreError::Validation(format!(
                "self edge {from:?} -> {from:?} would form a cycle"
            )));
        }
        let edge_count: usize = self.edges.values().map(BTreeSet::len).sum();
        let already = self.edges.get(from).is_some_and(|s| s.contains(to));
        if !already && edge_count >= limits.max_edges {
            return Err(CoreError::Validation(format!(
                "DAG cannot exceed {} edges",
                limits.max_edges
            )));
        }
        // Cycle check: adding from->to creates a cycle iff `to` already
        // reaches `from`.
        if Self::reachable(&self.edges, to, from) {
            return Err(CoreError::Validation(format!(
                "edge {from:?} -> {to:?} would create a cycle"
            )));
        }
        self.edges.entry(from.to_owned()).or_default().insert(to.to_owned());
        Ok(())
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.edges.values().map(BTreeSet::len).sum()
    }

    /// Deterministic topological order (Kahn's algorithm with BTreeSet
    /// frontier selection). Independent of insertion order.
    ///
    /// # Errors
    /// [`CoreError::Validation`] if a cycle exists — impossible through the
    /// checked API, kept as a guard for direct construction paths.
    pub fn topological_order(&self) -> Result<Vec<(String, &T)>, CoreError> {
        let mut indegree: BTreeMap<&str, usize> = self
            .nodes
            .keys()
            .map(|k| (k.as_str(), 0))
            .collect();
        for targets in self.edges.values() {
            for t in targets {
                *indegree.get_mut(t.as_str()).expect("edge endpoints validated") += 1;
            }
        }
        let mut ready: BTreeSet<&str> = indegree
            .iter()
            .filter(|(_, &d)| d == 0)
            .map(|(k, _)| *k)
            .collect();
        let mut order = Vec::with_capacity(self.nodes.len());
        while let Some(k) = ready.pop_first() {
            order.push(k);
            if let Some(targets) = self.edges.get(k) {
                for t in targets {
                    let deg =
                        indegree.get_mut(t.as_str()).expect("edge endpoints validated");
                    *deg -= 1;
                    if *deg == 0 {
                        ready.insert(t.as_str());
                    }
                }
            }
        }
        if order.len() != self.nodes.len() {
            return Err(CoreError::Validation(
                "cycle detected during topological sort".into(),
            ));
        }
        Ok(order
            .into_iter()
            .map(|k| {
                (
                    k.to_owned(),
                    self.nodes.get(k).expect("key originated here"),
                )
            })
            .collect())
    }

    fn reachable(
        edges: &BTreeMap<String, BTreeSet<String>>,
        start: &str,
        target: &str,
    ) -> bool {
        let mut stack = vec![start];
        let mut seen = BTreeSet::new();
        while let Some(current) = stack.pop() {
            if current == target {
                return true;
            }
            if seen.insert(current.to_owned()) {
                if let Some(nexts) = edges.get(current) {
                    stack.extend(nexts.iter().map(String::as_str));
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lim() -> DagLimits {
        DagLimits::default()
    }

    fn sample() -> Dag<&'static str> {
        // Mission example: A -> B, A? no: A -> B, B -> D, B -> C, C -> D
        let mut g = Dag::new();
        for k in ["a", "b", "c", "d"] {
            g.add_node(k, k, &lim()).expect("add");
        }
        g.add_edge("a", "b", &lim()).expect("edge");
        g.add_edge("b", "c", &lim()).expect("edge");
        g.add_edge("b", "d", &lim()).expect("edge");
        g.add_edge("c", "d", &lim()).expect("edge");
        g
    }

    #[test]
    fn topological_order_respects_dependencies() {
        let g = sample();
        let order: Vec<String> =
            g.topological_order().expect("acyclic").into_iter().map(|(k, _)| k).collect();
        assert_eq!(order.len(), 4);
        let pos = |n: &str| order.iter().position(|x| x == n).expect("present");
        assert!(pos("a") < pos("b"));
        assert!(pos("b") < pos("c"));
        assert!(pos("b") < pos("d"));
        assert!(pos("c") < pos("d"));
    }

    #[test]
    fn order_is_insertion_independent() {
        let mut g1 = Dag::new();
        let mut g2 = Dag::new();
        let keys = ["a", "b", "c"];
        for k in keys {
            g1.add_node(k, (), &lim()).expect("add");
        }
        for k in keys.iter().rev() {
            g2.add_node(*k, (), &lim()).expect("add");
        }
        for g in [&mut g1, &mut g2] {
            g.add_edge("a", "b", &lim()).expect("edge");
            g.add_edge("b", "c", &lim()).expect("edge");
        }
        let o1: Vec<String> = g1.topological_order().expect("ok").into_iter().map(|(k, _)| k).collect();
        let o2: Vec<String> = g2.topological_order().expect("ok").into_iter().map(|(k, _)| k).collect();
        assert_eq!(o1, o2);
    }

    #[test]
    fn direct_cycle_rejected() {
        let mut g = Dag::new();
        g.add_node("a", (), &lim()).expect("add");
        g.add_node("b", (), &lim()).expect("add");
        g.add_edge("a", "b", &lim()).expect("edge");
        assert!(g.add_edge("b", "a", &lim()).is_err());
    }

    #[test]
    fn transitive_cycle_rejected() {
        let mut g = Dag::new();
        for k in ["a", "b", "c"] {
            g.add_node(k, (), &lim()).expect("add");
        }
        g.add_edge("a", "b", &lim()).expect("edge");
        g.add_edge("b", "c", &lim()).expect("edge");
        // c reaches a already? No: a->b->c. Adding c->a closes the loop.
        assert!(g.add_edge("c", "a", &lim()).is_err());
    }

    #[test]
    fn self_cycle_rejected() {
        let mut g = Dag::new();
        g.add_node("a", (), &lim()).expect("add");
        assert!(g.add_edge("a", "a", &lim()).is_err());
    }

    #[test]
    fn unknown_endpoints_rejected() {
        let mut g = Dag::new();
        g.add_node("a", (), &lim()).expect("add");
        assert!(g.add_edge("a", "ghost", &lim()).is_err());
    }

    #[test]
    fn node_limit_enforced() {
        let tight = DagLimits { max_nodes: 2, ..DagLimits::default() };
        let mut g = Dag::new();
        g.add_node("a", (), &tight).expect("first");
        g.add_node("b", (), &tight).expect("second");
        assert!(g.add_node("c", (), &tight).is_err());
    }
}
