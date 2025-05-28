<!-- filepath: /Users/taigaishida/workspace/bako/README.md -->
# Bako - Drag, Drop, Knowledge

Bako is a project designed to create a local knowledge base by watching file system changes, processing files, and generating embeddings.

## Future Vision

Bako aims to evolve beyond a single-instance application into a distributed knowledge management system. The key aspects of this vision include:

*   **Distributed Nodes**: Deploy Bako (Rust core) instances on multiple computers, devices, or servers. Each node will monitor local file changes, process data, and generate embeddings independently.
*   **Central Knowledge Base**: Data (metadata, embeddings, and potentially processed content) from all distributed nodes will be synchronized and aggregated into a central knowledge base.
*   **MCP Server**: The Bako Server (Python) (currently heavily under construction) will serve as the central MCP server. This server will provide a standardized interface for querying and interacting with the aggregated knowledge from all connected nodes.
*   **Scalability and Collaboration**: This architecture will allow for a massively scalable knowledge base, enabling teams or individuals to collaboratively build and share knowledge across different locations and devices.
*   **Unified Access**: Users will be able to access the entire distributed knowledge base through the central MCP server, regardless of where the data originated.

This distributed model will empower users to create powerful, interconnected knowledge ecosystems.

## Core Functionality

The core of Bako is a Rust application responsible for:
*   Monitoring a specified directory for file creations, modifications, and deletions on individual nodes.
*   Reading and processing various file types.
*   Generating embeddings for text-based content.
*   Storing file metadata and embeddings in a local SQLite database.

The goal is to allow users to easily build and query a personal or project-specific knowledge base by simply managing files in a directory.

## Features

*   **File System Watcher**: Automatically detects changes in designated directories.
*   **File Processing**: Extracts content from various file types (details to be specified).
*   **Embedding Generation**: Creates vector embeddings for text content, enabling semantic understanding.
*   **Persistent Storage**: Uses SQLite to store file information and their embeddings.
*   **Local First**: Operates primarily on local data, ensuring privacy and control.

## Getting Started

**Platform Support:** Bako has been primarily tested on **macOS**. While other platforms may work, they are not yet officially confirmed.

### Prerequisites

*   Rust toolchain (latest stable recommended)
*   (Potentially) An OpenAI API key if using OpenAI models for embeddings.

### Installation & Running

**1. Bako (Rust Core):**

Navigate to the project root (`bako/`):

```bash
# Build the Rust application
cargo build

# Run the Rust application
cargo run
```

**Configuration:**

Bako uses a `config.toml` file for its settings. You will need to create this file manually in the specified location if it does not already exist.

**Location:**

First, ensure the application configuration directory exists. If not, you'll need to create it:

*   **macOS:** `/Users/<YourUserName>/Library/Application Support/io.tonythetaiga.bako/`
*   **Linux:** (Typically) `/home/<YourUserName>/.config/io.tonythetaiga.bako/`
*   **Windows:** (Typically) `C:\Users\<YourUserName>\AppData\Roaming\io.tonythetaiga.bako\`

(The exact path might vary slightly based on your operating system and environment.)

Inside this `io.tonythetaiga.bako` directory, create a file named `config.toml`.

**Example `config.toml` content:**

```toml
db_path = "bako.db" # Path to the SQLite database file. Can be an absolute path or relative (e.g., to this config directory). (Note: Manually setting this is temporary for early alpha versions.)
watch_directory = "/path/to/your/watched/folder" # IMPORTANT: Change this to the directory you want Bako to monitor.
watcher_poll_duration_secs = 5 # How often to poll for file system changes (in seconds).
queue_process_interval_secs = 10 # How often to process the queue of changed files.
queue_batch_size = 100 # The number of files to process in each batch.
```

**Instructions:**

1.  Create the `io.tonythetaiga.bako` directory if it doesn't already exist at the path appropriate for your OS.
2.  Create a new file named `config.toml` inside the `io.tonythetaiga.bako` directory.
3.  Copy the example content above into your `config.toml`.
4.  **Crucially, change the `watch_directory`** to the actual folder you want Bako to monitor.
You may also adjust other settings like `db_path` as needed.

## Usage

1.  Start the Bako Rust application. It will begin monitoring the configured directory.
2.  Add, modify, or delete files in the monitored directory. Bako will process these changes, generate embeddings for new or updated text content, and store the information in `bako.db`.
