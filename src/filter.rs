use globset::{Glob, GlobSet, GlobSetBuilder};

#[derive(Clone)]
pub struct Filter {
    include: GlobSet,
    exclude: GlobSet,
}

impl Filter {
    pub fn new(include: &[String], exclude: &[String]) -> anyhow::Result<Self> {
        let empty = vec!["**/*".to_string()];
        let include_patterns = if include.is_empty() { &empty } else { include };

        let mut include_builder = GlobSetBuilder::new();
        for p in include_patterns {
            include_builder.add(Glob::new(p)?);
        }

        let mut exclude_builder = GlobSetBuilder::new();
        for p in exclude {
            exclude_builder.add(Glob::new(p)?);
        }

        Ok(Self {
            include: include_builder.build()?,
            exclude: exclude_builder.build()?,
        })
    }

    pub fn matches(&self, path: &str) -> bool {
        let included = self.include.is_match(path);
        let excluded = self.exclude.is_match(path);
        included && !excluded
    }
}