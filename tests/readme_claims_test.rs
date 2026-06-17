use std::fs;
use std::path::Path;

#[test]
fn test_readme_claims_have_evidence() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme_path = manifest_dir.join("README.md");
    let claims_path = manifest_dir.join("docs/claims.md");

    assert!(
        readme_path.exists(),
        "README.md not found at {:?}",
        readme_path
    );
    assert!(
        claims_path.exists(),
        "docs/claims.md not found at {:?}",
        claims_path
    );

    let readme_content = fs::read_to_string(readme_path).unwrap();
    let claims_content = fs::read_to_string(claims_path).unwrap();

    let mut checked_claims = Vec::new();
    for line in readme_content.lines() {
        let trimmed = line.trim();
        if let Some(stripped) = trimmed.strip_prefix("- [x]") {
            // Extract the claim title.
            // Example 1: - [x] **No Subprocess Orchestration**: Enforced...
            // Example 2: - [x] [No Subprocess Orchestration](docs/claims.md#no-subprocess-orchestration)
            let remaining = stripped.trim();
            let title = if let Some(inner) = remaining.strip_prefix('[') {
                if let Some(end_idx) = inner.find(']') {
                    &inner[..end_idx]
                } else {
                    remaining
                }
            } else if let Some(inner) = remaining.strip_prefix("**") {
                if let Some(end_idx) = inner.find("**") {
                    &inner[..end_idx]
                } else {
                    remaining
                }
            } else {
                remaining
            };

            // Clean title of any trailing characters like ':'
            let clean_title = title.trim_end_matches(':').trim().to_string();
            checked_claims.push(clean_title);
        }
    }

    assert!(
        !checked_claims.is_empty(),
        "No checked claims found in README.md"
    );

    for claim in checked_claims {
        let heading = format!("## {}", claim);
        assert!(
            claims_content.contains(&heading),
            "Claim '{}' is checked [x] in README.md but has no matching heading '{}' in docs/claims.md",
            claim,
            heading
        );
    }
}
