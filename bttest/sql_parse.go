package bttest

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Compiled patterns for CMV SQL parsing. Bigtable CMV queries follow a
// restricted GoogleSQL subset using SPLIT(_key, sep)[SAFE_OFFSET(n)] AS alias
// to extract row key components.
var (
	reFrom    = regexp.MustCompile("(?i)FROM\\s+`([^`]+)`")
	reGroupBy = regexp.MustCompile(`(?i)\bGROUP\s+BY\b`)

	// Matches: SPLIT(_key, '#')[SAFE_OFFSET(3)] AS alias
	reSplitOffset = regexp.MustCompile(
		`(?i)SPLIT\(_key,\s*'([^']+)'\)\[SAFE_OFFSET\((\d+)\)\]\s+AS\s+(\w+)`)

	reKeyAlias = regexp.MustCompile(`(?i)\b_key\s+AS\s+(\w+)`)
	reOrderBy  = regexp.MustCompile(`(?i)ORDER\s+BY\s+(.+)$`)
)

// ParseCMVConfigFromSQL extracts a CMVConfig from a Bigtable CMV SQL query.
//
// Only ORDER BY (secondary index) queries are supported. GROUP BY
// (aggregation) queries require maintaining running aggregates and are
// not implemented by the emulator.
//
// WHERE clauses are silently ignored; the emulator propagates all source
// writes to the CMV regardless of any filter predicate.
//
// Supported SELECT column forms:
//
//	SPLIT(_key, '<sep>')[SAFE_OFFSET(<n>)] AS <alias>
//	_key AS <alias>              → sets AppendSourceKey when alias is in ORDER BY
//	<family> AS <family>         → adds to IncludeFamilies
//
// The ORDER BY clause determines the CMV key component ordering.
func ParseCMVConfigFromSQL(viewID, query string) (*CMVConfig, error) {
	if reGroupBy.MatchString(query) {
		return nil, fmt.Errorf("GROUP BY (aggregation) queries are not supported; only ORDER BY (key re-mapping) CMVs are emulated")
	}

	cfg := &CMVConfig{ViewID: viewID}

	sourceTable, err := parseSourceTable(query)
	if err != nil {
		return nil, err
	}
	cfg.SourceTable = sourceTable

	colMap, sep, err := parseSplitColumns(query)
	if err != nil {
		return nil, err
	}
	cfg.KeySeparator = sep

	sourceKeyAlias := parseSourceKeyAlias(query)

	keyMapping, appendSourceKey, err := parseOrderBy(query, colMap, sourceKeyAlias)
	if err != nil {
		return nil, err
	}
	cfg.KeyMapping = keyMapping
	cfg.AppendSourceKey = appendSourceKey

	cfg.IncludeFamilies = parseFamilies(query, colMap)

	return cfg, nil
}

// parseSourceTable extracts the table name from "FROM `table_name`".
func parseSourceTable(query string) (string, error) {
	m := reFrom.FindStringSubmatch(query)
	if m == nil {
		return "", fmt.Errorf("could not parse FROM clause in CMV query")
	}
	return m[1], nil
}

// parseSplitColumns extracts SPLIT(_key, sep)[SAFE_OFFSET(n)] AS alias
// expressions and returns a map of alias → offset index plus the separator.
func parseSplitColumns(query string) (colMap map[string]int, sep string, err error) {
	matches := reSplitOffset.FindAllStringSubmatch(query, -1)
	if len(matches) == 0 {
		return nil, "", fmt.Errorf("could not parse SPLIT/SAFE_OFFSET expressions in CMV query")
	}
	colMap = make(map[string]int, len(matches))
	for _, m := range matches {
		sep = m[1]
		idx, _ := strconv.Atoi(m[2])
		colMap[m[3]] = idx
	}
	return colMap, sep, nil
}

// parseSourceKeyAlias detects "_key AS <alias>" in the SELECT clause.
// Returns the alias if found, empty string otherwise.
func parseSourceKeyAlias(query string) string {
	if m := reKeyAlias.FindStringSubmatch(query); m != nil {
		return m[1]
	}
	return ""
}

// parseOrderBy processes the ORDER BY clause to build the key mapping.
// Columns that reference SPLIT aliases become key mapping entries;
// _key or its alias sets appendSourceKey.
func parseOrderBy(query string, colMap map[string]int, sourceKeyAlias string) (keyMapping []int, appendSourceKey bool, err error) {
	m := reOrderBy.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		return nil, false, fmt.Errorf("could not parse ORDER BY clause in CMV query")
	}
	for _, col := range strings.Split(m[1], ",") {
		col = strings.TrimSpace(col)
		if col == "_key" || (sourceKeyAlias != "" && col == sourceKeyAlias) {
			appendSourceKey = true
			continue
		}
		idx, ok := colMap[col]
		if !ok {
			return nil, false, fmt.Errorf("ORDER BY column %q not found in SELECT", col)
		}
		keyMapping = append(keyMapping, idx)
	}
	if len(keyMapping) == 0 {
		return nil, false, fmt.Errorf("no key mapping columns found in ORDER BY")
	}
	return keyMapping, appendSourceKey, nil
}

// parseFamilies extracts column family inclusions from the SELECT clause.
// A family is identified by the pattern "<name> AS <name>" where both sides
// match and the name is not a SPLIT column alias or "_key".
func parseFamilies(query string, colMap map[string]int) []string {
	famRe := regexp.MustCompile(`(?:,\s*)(\w+)\s+AS\s+(\w+)`)
	matches := famRe.FindAllStringSubmatch(query, -1)
	var families []string
	for _, m := range matches {
		src, alias := m[1], m[2]
		if src == alias && src != "_key" {
			if _, isCol := colMap[src]; !isCol {
				families = append(families, src)
			}
		}
	}
	return families
}
