# Generic list parsing that works on different sections of the toml file
extract_list toml_file section list_name:
  awk -v section="{{section}}" -v list_name="{{list_name}}" ' \
  /^\[.*\]/ {in_section=($0 == "[" section "]") ? 1 : 0} \
  in_section && $0 ~ ("^" list_name " = \\[") {in_list=1; next} \
  in_list && /\]/ {in_list=0} \
  in_list { \
    line=$0; gsub(/^[ \t"]+|[" ,]+$/, "", line); \
    if (line !~ /^#/) { if (line ~ /:/) sub(/:.*/, "", line); print line } \
  }' {{toml_file}}


