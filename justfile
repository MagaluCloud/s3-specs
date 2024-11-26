container_runner := env_var_or_default("CONTAINER_RUNNER", "docker")
s3_tester_image := "ghcr.io/marmotitude/s3-tester:tests"
s3_tester_default_command := "test.sh --profiles default --clients aws,mgc --tests 1,2,53,64"

# Choose recipies interactively
_default:
  @just --choose "fzf"

# List the available recipies
list:
  just --list

# Run the "s3-tester:tests" image passing any command config_file is a yaml in the legacy profiles.yaml format
legacy-run config_file *command=s3_tester_default_command:
  {{container_runner}} run -t -e PROFILES="$(cat {{config_file}})" {{s3_tester_image}} {{command}}

# Choose and run test categories or IDs from s3-tester
legacy:
  #!/usr/bin/env bash
  # Prompt for the profiles.yaml location
  echo "Enter the location of the profiles.yaml config file (default: ../s3-tester/profiles.yaml):"
  read -r config_file
  config_file=${config_file:-../s3-tester/profiles.yaml}
  echo "Using config file: $config_file"

  # Prompt for profiles
  echo "Enter a comma-separated list of profiles (default: br-ne1,br-se1):"
  read -r profiles
  profiles=${profiles:-br-ne1,br-se1}
  echo "You entered profiles: $profiles"

  # Prompt for clients
  clients_list="aws\nmgc\nrclone"
  selected_clients=$(echo -e "$clients_list" | fzf --multi --prompt="Select a client: " --height=13 --border | paste -sd ',' -)
  echo "You selected the following clients: $selected_clients"

  # Choose between categories or test IDs
  echo "Do you want to pass categories or test IDs? (c for categories, t for test IDs):"
  read -r input_type

  if [ "$input_type" = "c" ]; then
    # Prompt for categories
    category_list=$(./bin/list_markers.py --legacy)
    selected_categories=$(echo "$category_list" | fzf --multi --prompt="Select a category: " --height=16 --border | paste -sd ',' -)
    echo "You selected the following categories: $selected_categories"

    # Call the legacy-category-test recipe
    just legacy-category-test "$config_file" "$profiles" "$selected_clients" "$selected_categories"

  elif [ "$input_type" = "t" ]; then
    # Prompt for test IDs
    echo "Enter a comma-separated list of test IDs:"
    read -r test_ids
    echo "You entered test IDs: $test_ids"

    # Call the legacy-idlist-test recipe
    just legacy-idlist-test "$config_file" "$profiles" "$selected_clients" "$test_ids"

  else
    echo "Invalid input. Please run the command again and choose 'c' for categories or 't' for test IDs."
    exit 1
  fi

# Run s3-tester shellspec tests passing profiles, clients, and categories lists.
legacy-category-test config_file profiles clients categories: (legacy-run config_file "test.sh --profiles" profiles "--clients" clients "--categories" categories)

# Run s3-tester shellspec tests passing profiles, clients, and test ID lists.
legacy-idlist-test config_file profiles clients tests: (legacy-run config_file "test.sh --profiles" profiles "--clients" clients "--tests" tests)
