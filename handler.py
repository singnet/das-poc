import json
import time
from function.das.distributed_atom_space import DistributedAtomSpace, QueryOutputFormat
from function.das.logical_expression_parser import LogicalExpressionParser

import logging
logging.basicConfig(level=logging.INFO)

def handle(request_json, context = {}):
    try:
        start_time = time.time()
        response = {}
        success = "Action executed successfully."

        try:
            request = json.loads(request_json)

            if request is None:
                raise Exception("Invalid JSON.")
        except Exception as e:
            response["msg"] = "Invalid JSON."
            response_json = json.dumps(response)
            print(response_json)
            return

        database_name, action = None, None
        try:
            database_name = request.get('database_name')
            if database_name is None:
                raise Exception("Property 'database_name' is missing.")
            logging.info(f"database_name: {database_name}")
            print(f"database_name: {database_name}")

            action = request.get('action')
            if action is None:
                raise Exception("Property 'action' is missing.")
        except Exception as e:
            response["msg"] = str(e)
            response_json = json.dumps(response)
            print(response_json)
            return

        das = DistributedAtomSpace(database_name=database_name)
        if action == "get_node":
            node_type = request.get('node_type')
            node_name = request.get('node_name')
            result = das.get_node(node_type=node_type, node_name=node_name, output_format=QueryOutputFormat.JSON)

            response["msg"] = success
            response["result"] = result
        elif action == "get_nodes":
            node_type = request.get('node_type')
            node_name = request.get('node_name')
            result = das.get_nodes(
                node_type=node_type, 
                node_name=node_name, 
                output_format=QueryOutputFormat.JSON
            )

            response["msg"] = success
            response["result"] = result
        elif action == "get_link":
            link_type = request.get('link_type')
            targets = request.get('targets')
            result = das.get_link(link_type= link_type, targets=targets, output_format=QueryOutputFormat.JSON) 

            response["msg"] = success
            response["result"] = result
        elif action == "get_links":
            link_type = request.get('link_type')
            targets = request.get('targets')
            target_types = request.get('target_types')
            result = das.get_links(link_type= link_type, targets=targets, output_format=QueryOutputFormat.JSON, target_types=target_types) 

            response["msg"] = success
            response["result"] = result
        elif action == "get_link_type":
            link_type = request.get('link_type')
            result = das.get_link_type(link_handle=link_type) 

            response["msg"] = success
            response["result"] = result
        elif action == "get_link_targets":
            link_targets = request.get('link_targets')
            result = das.get_link_targets(link_handle=link_targets) 

            response["msg"] = success
            response["result"] = result
        elif action == "get_node_type":
            node_type = request.get('node_type')
            result = das.get_node_type(node_handle=node_type) 

            response["msg"] = success
            response["result"] = result
        elif action == "get_node_name":
            node_name = request.get('node_name')
            result = das.get_node_name(node_handle=node_name) 

            response["msg"] = success
            response["result"] = result
        elif action == "clear_database":
            das.clear_database() 

            response["msg"] = success
        elif action == "count_atoms":
            result = das.count_atoms() 

            response["msg"] = success
            response["result"] = result
        elif action == "get_atom":
            handle = request.get('handle')
            result = das.get_atom(
                handle=handle,
                output_format=QueryOutputFormat.JSON
            )

            response["msg"] = success
            response["result"] = result
        
        elif action == "query":
            query_dict = request.get('query', dict())

            logical_expression_parser = LogicalExpressionParser()
            logical_expression = logical_expression_parser.from_dict(query_dict)

            result = das.query(logical_expression, QueryOutputFormat.JSON)
            response["msg"] = success
            response["result"] = result

        else:
            response["msg"] = "Unknown action: " + action

        end_time = time.time()
        response["time_in_seconds"] = end_time - start_time

        response_json = json.dumps(response)
        print(response_json)
    except Exception as e:
        print(e)
        response = {
            "msg": "An internal error occurred. Try again later."
        }

        response_json = json.dumps(response)
        print(response_json)
