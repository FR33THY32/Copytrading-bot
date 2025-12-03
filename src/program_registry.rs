use crate::parsers::{
    pump_amm::PumpAmmParser,
    pump_fun::{PumpFunParser, PUMP_FUN_PROGRAM_ID},
    ProgramParser,
};
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry that holds all program parsers
#[derive(Debug)]
pub struct ProgramRegistry {
    parsers: HashMap<Pubkey, Arc<dyn ProgramParser>>,
}

impl ProgramRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            parsers: HashMap::new(),
        }
    }

    /// Create a registry with default parsers
    pub fn with_defaults() -> Self {
        let mut registry = Self::new();

        // Register default parsers
        registry.register_parser(Arc::new(PumpFunParser::new()));
        registry.register_parser(Arc::new(PumpAmmParser::new()));

        // Add more default parsers here as they're implemented
        // registry.register_parser(Arc::new(RaydiumParser::new()));
        // registry.register_parser(Arc::new(JupiterParser::new()));

        registry
    }

    /// Register a parser for a program
    pub fn register_parser(&mut self, parser: Arc<dyn ProgramParser>) {
        let program_id = parser.program_id();
        self.parsers.insert(program_id, parser);
    }

    /// Get a parser for a specific program
    pub fn get_parser(&self, program_id: &Pubkey) -> Option<&Arc<dyn ProgramParser>> {
        self.parsers.get(program_id)
    }

    /// Check if a parser exists for a program
    pub fn has_parser(&self, program_id: &Pubkey) -> bool {
        self.parsers.contains_key(program_id)
    }

    /// Get the number of registered parsers
    pub fn len(&self) -> usize {
        self.parsers.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.parsers.is_empty()
    }

    /// Get an iterator over all registered parsers
    pub fn iter(&self) -> impl Iterator<Item = (&Pubkey, &Arc<dyn ProgramParser>)> {
        self.parsers.iter()
    }

    /// Get all registered program IDs
    pub fn program_ids(&self) -> Vec<Pubkey> {
        self.parsers.keys().copied().collect()
    }

    /// Remove a parser from the registry
    pub fn remove_parser(&mut self, program_id: &Pubkey) -> Option<Arc<dyn ProgramParser>> {
        self.parsers.remove(program_id)
    }

    /// Clear all parsers from the registry
    pub fn clear(&mut self) {
        self.parsers.clear();
    }
}

impl Default for ProgramRegistry {
    fn default() -> Self {
        Self::with_defaults()
    }
}

/// Builder pattern for creating a registry with specific parsers
pub struct ProgramRegistryBuilder {
    parsers: Vec<Arc<dyn ProgramParser>>,
}

impl ProgramRegistryBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            parsers: Vec::new(),
        }
    }

    /// Add a parser to the builder
    pub fn with_parser(mut self, parser: Arc<dyn ProgramParser>) -> Self {
        self.parsers.push(parser);
        self
    }

    /// Add the default parsers
    pub fn with_defaults(mut self) -> Self {
        self.parsers.push(Arc::new(PumpFunParser::new()));
        self.parsers.push(Arc::new(PumpAmmParser::new()));
        // Add more default parsers here
        self
    }

    /// Build the registry
    pub fn build(self) -> ProgramRegistry {
        let mut registry = ProgramRegistry::new();
        for parser in self.parsers {
            registry.register_parser(parser);
        }
        registry
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry_creation() {
        let registry = ProgramRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_with_defaults() {
        let registry = ProgramRegistry::with_defaults();
        assert!(!registry.is_empty());
        assert!(registry.has_parser(&PUMP_FUN_PROGRAM_ID));
    }

    #[test]
    fn test_register_parser() {
        let mut registry = ProgramRegistry::new();
        let parser = Arc::new(PumpFunParser::new());
        registry.register_parser(parser);

        assert_eq!(registry.len(), 1);
        assert!(registry.has_parser(&PUMP_FUN_PROGRAM_ID));
        assert!(registry.get_parser(&PUMP_FUN_PROGRAM_ID).is_some());
    }

    #[test]
    fn test_remove_parser() {
        let mut registry = ProgramRegistry::with_defaults();
        let initial_len = registry.len();

        let removed = registry.remove_parser(&PUMP_FUN_PROGRAM_ID);
        assert!(removed.is_some());
        assert_eq!(registry.len(), initial_len - 1);
        assert!(!registry.has_parser(&PUMP_FUN_PROGRAM_ID));
    }

    #[test]
    fn test_registry_builder() {
        let registry = ProgramRegistryBuilder::new()
            .with_parser(Arc::new(PumpFunParser::new()))
            .build();

        assert_eq!(registry.len(), 1);
        assert!(registry.has_parser(&PUMP_FUN_PROGRAM_ID));
    }

    #[test]
    fn test_registry_builder_with_defaults() {
        let registry = ProgramRegistryBuilder::new().with_defaults().build();

        assert!(!registry.is_empty());
        assert!(registry.has_parser(&PUMP_FUN_PROGRAM_ID));
    }
}
