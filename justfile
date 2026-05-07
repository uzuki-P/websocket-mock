set dotenv-load

default:
    @just --list

# Install dependencies
install:
    npm install

# Start the server
start:
    node server.js

# Start with file watching (auto-restart on changes)
dev:
    node --watch server.js

# Start on a custom port (usage: just port 4000)
port p:
    DASHBOARD_PORT={{p}} node server.js

# Start with custom WS port (usage: just ws-port 9000)
ws-port p:
    node -e "process.env.WS_DEFAULT_PORT='{{p}}'; require('./server.js')"
