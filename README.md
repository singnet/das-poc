# Distributed Atom Space (DAS)

This repo aims to develop a new design to store all the MeTTa expressions in a database to be accessed through an API.

## Functions

The `handle` function is responsible for handling various actions related to a Distributed Atom Space (DAS) database. It takes a JSON request as input and performs the requested action on the database.

## Parameters

- `request_json` (str): A JSON-formatted string containing the request data. This parameter is required.

- `context` (dict): An optional parameter that can be used to provide additional context or information to the function. It is typically not required for basic usage.

## Actions and Request Format

The `handle` function supports several actions, each with its specific set of parameters. Below is a list of supported actions and their request formats:

1. **Get Node**

   - Action: `"get_node"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `node_type` (str): The type of the node.
     - `node_name` (str): The name of the node.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_node",
       "node_type": "Person",
       "node_name": "JohnDoe"
     }
     ```

2. **Get Nodes**

   - Action: `"get_nodes"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `node_type` (str): The type of the nodes.
     - `node_name` (str): The name of the nodes (optional).
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_nodes",
       "node_type": "Person",
       "node_name": "JohnDoe"
     }
     ```

3. **Get Link**

   - Action: `"get_link"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `link_type` (str): The type of the link.
     - `targets` (list): A list of target nodes or handles.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_link",
       "link_type": "Friend",
       "targets": ["JohnDoe", "JaneSmith"]
     }
     ```

4. **Get Links**

   - Action: `"get_links"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `link_type` (str): The type of the link.
     - `targets` (list): A list of target nodes or handles.
     - `target_types` (list): A list of target node types (optional).
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_links",
       "link_type": "Friend",
       "targets": ["JohnDoe", "JaneSmith"],
       "target_types": ["Person"]
     }
     ```

5. **Get Link Type**

   - Action: `"get_link_type"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `link_type` (str): The type of the link.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_link_type",
       "link_type": "Friend"
     }
     ```

6. **Get Link Targets**

   - Action: `"get_link_targets"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `link_targets` (str): The handle of the link.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_link_targets",
       "link_targets": "12345"
     }
     ```

7. **Get Node Type**

   - Action: `"get_node_type"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `node_type` (str): The type of the node.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_node_type",
       "node_type": "Person"
     }
     ```

8. **Get Node Name**

   - Action: `"get_node_name"`
   - Parameters:
     - `database_name` (str): The name of the database.
     - `node_name` (str): The handle of the node.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "get_node_name",
       "node_name": "JohnDoe"
     }
     ```

9. **Clear Database**

   - Action: `"clear_database"`
   - Parameters:
     - `database_name` (str): The name of the database.
   - Example Request:
     ```json
     {
       "database_name": "my_database",
       "action": "clear_database"
     }
     ```

10. **Count Atoms**

    - Action: `"count_atoms"`
    - Parameters:
      - `database_name` (str): The name of the database.
    - Example Request:
      ```json
      {
        "database_name": "my_database",
        "action": "count_atoms"
      }
      ```

11. **Get Atom**

    - Action: `"get_atom"`
    - Parameters:
      - `database_name` (str): The name of the database.
      - `handle` (str): The handle of the atom.
    - Example Request:
      ```json
      {
        "database_name": "my_database",
        "action": "get_atom",
        "handle": "12345"
      }
      ```

12. **Query**
    - Action: `"query"`
    - Parameters:
      - `database_name` (str): The name of the database.
      - `query` (dict): A dictionary representing a logical expression query.
    - Example Request:
      ```json
      {
        "database_name": "my_database",
        "action": "query",
        "query": {
          "And": [
            {
              "Link": {
                "link_type": "Evaluation",
                "ordered": true,
                "targets": [
                  {
                    "Variable": {
                      "variable_name": "000000214999369af91fb563b4e0eadb"
                    }
                  },
                  {
                    "Variable": {
                      "variable_name": "1a0738cb1a6b6b8ce7bae84c4296c0ce"
                    }
                  }
                ]
              }
            }
          ]
        }
      }
      ```

## Response Format

The `handle` function returns a JSON response with the following format:

- `msg` (str): A message indicating the result of the action.
- `result` (varies): The result of the action, if applicable (e.g., query results).
- `time_in_seconds` (float): The time taken to execute the action in seconds.

## Error Handling

If an error occurs during the execution of an action, the response will contain an error message in the `msg` field.
