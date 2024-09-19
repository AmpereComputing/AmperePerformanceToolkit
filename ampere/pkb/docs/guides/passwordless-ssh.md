## How To Setup SSH and Passwordless Authentication
Complete guide can be found at [SSH-Academy](https://www.ssh.com/academy/ssh/copy-id)
### Generate an SSH Key
1. From your terminal on the APT Runner, run `ssh-keygen -t rsa`
2. Choose the location where you want this key to be stored
    - The default location is `$HOME/.ssh/key-name`
    - This is generally sufficient
3. Choose the name of your key
    - By default the key will be named `id_rsa`
    - You can name the key whatever you like
4. Skip adding a passphrase and key will be created

### Add Passwordless SSH
1. Copy the public key created on the APT Runner system with `ssh-copy-id -i $HOME/.ssh/key-name user@domain` 
2. run `ssh user@domain` to test configuration.
