# Generic list parsing that works on different sections of the toml file
_extract_list toml_file section list_name:
  awk -v section="{{section}}" -v list_name="{{list_name}}" ' \
  /^\[.*\]/ {in_section=($0 == "[" section "]") ? 1 : 0} \
  in_section && $0 ~ ("^" list_name " = \\[") {in_list=1; next} \
  in_list && /\]/ {in_list=0} \
  in_list { \
    line=$0; gsub(/^[ \t"]+|[" ,]+$/, "", line); \
    if (line !~ /^#/) { if (line ~ /:/) sub(/:.*/, "", line); print line } \
  }' {{toml_file}}

# List legacy categories (shellspec tags of legacy s3-tester)
_legacy-categories:
  just _extract_list "pyproject.toml" "tool.s3-tester" "markers"

# Run legacy s3-tester tests using docker or podman (oci_runner)
@_legacy-tests config_file profiles clients categories oci_runner="docker":
  echo Please run the following command:
  echo {{oci_runner}} run -t \
    -e PROFILES=\"\$\(cat {{config_file}}\)\" \
    {{s3_tester_image}} test.sh \
    --profiles {{profiles}} \
    --clients {{clients}} \
    --categories {{categories}}
