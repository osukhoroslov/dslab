use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::str::FromStr;

use crate::scheduler::Scheduler;
use crate::schedulers::dls::DlsScheduler;
use crate::schedulers::heft::HeftScheduler;
use crate::schedulers::lookahead::LookaheadScheduler;
use crate::schedulers::peft::PeftScheduler;
use crate::schedulers::portfolio_scheduler::PortfolioScheduler;
use crate::schedulers::simple_scheduler::SimpleScheduler;

use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct SchedulerParams {
    name: String,
    params: BTreeMap<String, String>,
}

impl SchedulerParams {
    pub fn from_str(s: &str) -> Option<Self> {
        let open = s.find('[');
        if open.is_none() {
            return Some(Self {
                name: s.to_string(),
                params: BTreeMap::new(),
            });
        }

        let open = open.unwrap();
        if !s.ends_with(']') {
            return None;
        }

        let mut params = BTreeMap::new();
        for param in s[open + 1..s.len() - 1].split(',') {
            let pos = param.find('=')?;
            params.insert(param[..pos].to_string(), param[pos + 1..].to_string());
        }

        Some(Self {
            name: s[..open].to_string(),
            params,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn get<T: FromStr, K: AsRef<str>>(&self, name: K) -> Option<T> {
        self.params.get(name.as_ref()).and_then(|s| s.parse().ok())
    }
}

impl std::fmt::Display for SchedulerParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.params.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(
                f,
                "{}[{}]",
                self.name,
                self.params.iter().map(|(k, v)| format!("{k}={v}")).join(",")
            )
        }
    }
}

pub fn default_scheduler_resolver(params: &SchedulerParams) -> Option<Rc<RefCell<dyn Scheduler>>> {
    match params.name.as_ref() {
        "Simple" => Some(Rc::new(RefCell::new(SimpleScheduler::new()))),
        "Heft" => Some(Rc::new(RefCell::new(HeftScheduler::from_scheduler_params(params)))),
        "Lookahead" => Some(Rc::new(RefCell::new(LookaheadScheduler::from_scheduler_params(params)))),
        "Peft" => Some(Rc::new(RefCell::new(PeftScheduler::from_scheduler_params(params)))),
        "Dls" => Some(Rc::new(RefCell::new(DlsScheduler::from_scheduler_params(params)))),
        "Portfolio" => Some(Rc::new(RefCell::new(PortfolioScheduler::from_scheduler_params(params)))),
        _ => None,
    }
}
