# Glossary

<span id="profile">profile</span>: A profile is a set of configs, such as region and endpoint, and credentials, such as api_key_id and api_secret_key. On aws cli and sdks, the profile have a name and the data lives on user config directories like `~/.aws/config` and `~/.aws/credentials`. Those tools may accept an environment variable `AWS_PROFILE` or an argument `--profile`