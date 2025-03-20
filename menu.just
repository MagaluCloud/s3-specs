_default:
  just --list menu

# TUI for picking multiple choice using fzf
_menu_picker title:
  fzf -m --header="{{title}} Tab=select, Enter=OK" --layout=reverse

# TUI for free text input using fzf
_text_prompt title default_value:
  echo {{default_value}} | \
  fzf --print-query --prompt="{{title}}" --layout=reverse | \
  tail -n1

# Interactively pick categories to test
test config_file="./params.example.yaml":
  #!/usr/bin/env bash
  test_categories=$(just categories | just menu::_menu_picker \
      "Select the categories to test" | sed ':a;N;$!ba;s/\n/ or /g')
  just _run_tests {{config_file}} "-m '$test_categories'" 

# Interactively pick legacy categories to test on s3-tester (soon to be deprecated)
legacy-test:
  #!/usr/bin/env bash
  config_file=$(\
    just menu::_text_prompt "Enter legacy config yaml file path: " "../s3-tester/profiles.yaml" \
  )
  profiles=$(\
    just menu::_text_prompt "Enter comma separated list of profile names: " "br-ne1,br-se1" \
  )
  categories=$(\
    just _legacy-categories | \
    just menu::_menu_picker \
      "Select the legacy categories to test on s3-tester" | \
    paste -sd ',' -\
  )
  clients=$(\
    echo -e "aws\nmgc\nrclone" | \
    just menu::_menu_picker \
      "Select the CLI clients" | \
    paste -sd ',' -\
  )

  echo "Using Config File: $config_file"
  echo "Using Profiles: $profiles"
  echo "Selected Clients: $clients"
  echo "Selected Categories: $categories"
  just _legacy-tests "$config_file" "$profiles" "$clients" "$categories" podman
