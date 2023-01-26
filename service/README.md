# gRPC wrapper for Distributed Atom Space (DAS)

In this module we implement a server and a client to allow the use of DAS through gRPC calls ([gRPC documentation](https://grpc.io/docs/)).

The gRPC server is a wrapper which exposes all the public API of DAS. The gRPC client is a CLI to make the calls through command line.

# How to use the CLI to manipulate a DAS

Later in this document we provide a step-by-step to build and run the server. For now, we assume there's a server running locally in the same machine.

A DAS gRPC server can store more them one Distributed Atom Space from different users. You can use `das-cli.sh` to manipulate your DAS. `das-cli.sh` is a Command-line interface with a number of commands to create, populate que query DAS.
If you didn't build a DAS yet you need to create a new one. You can do this using the command `create`:

```
$ ./scripts/das-cli.sh create --new-das-name my_knowledge_base
zkgftedbgvlwstjivhte
```

After `create` returns, you have a new empty DAS named `my_knowledge_base` created in the DAS gRPC server. The command returns a key (`zkgftedbgvlwstjivhte` in our example) which needs to be used in all the commands which manipulates or query this DAS.

Our DAS is empty, meaning that it has 0 nodes and 0 links. You can confirm this by calling:

```
$ /scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte count
(0, 0)
```

You can feed your knowledge base with MeTTa (`.metta`) or Atomese (`.scm`) files. Currently, the only way to do this is by calling the command `load` and passing a URL pointing to the file you want to be loaded.
Lets use a simple MeTTa example file we have in the service repository:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte load --url https://raw.githubusercontent.com/singnet/das/main/data/samples/animals.metta
Load request submitted. Check status using the command 'check'
```

NB the command output. It means the request is being processed by the server. Small knowledge bases like the one we're using in this example are read immediately but larger bases may take minutes or even hours to be loaded. While a knowledge base is being loaded, the DAS can't process any requests other than the command `check` which can be used exactly to figure out if a `load` command finished.

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte check
Ready
```

It's possible to load multiple-file knowledge bases using zip'ed or tar'ed bases. I.e. the file pointed by the URL can be a `.zip`, `.tar` or `.tgz` with a set of `.metta` or `.scm` files (no recursion on sub-directories).

Our example knowledge base is a simple one:

```
(: Similarity Type)
(: Concept Type)
(: Inheritance Type)
(: "human" Concept)
(: "monkey" Concept)
(: "chimp" Concept)
(: "snake" Concept)
(: "earthworm" Concept)
(: "rhino" Concept)
(: "triceratops" Concept)
(: "vine" Concept)
(: "ent" Concept)
(: "mammal" Concept)
(: "animal" Concept)
(: "reptile" Concept)
(: "dinosaur" Concept)
(: "plant" Concept)
(Similarity "human" "monkey")
(Similarity "human" "chimp")
(Similarity "chimp" "monkey")
(Similarity "snake" "earthworm")
(Similarity "rhino" "triceratops")
(Similarity "snake" "vine")
(Similarity "human" "ent")
(Inheritance "human" "mammal")
(Inheritance "monkey" "mammal")
(Inheritance "chimp" "mammal")
(Inheritance "mammal" "animal")
(Inheritance "reptile" "animal")
(Inheritance "snake" "reptile")
(Inheritance "dinosaur" "reptile")
(Inheritance "triceratops" "dinosaur")
(Inheritance "earthworm" "animal")
(Inheritance "rhino" "mammal")
(Inheritance "vine" "plant")
(Inheritance "ent" "plant")
```

![kbdiagram](documentation/kb_diagram.png 'Simple knowledge base')

Let's check the node/link count again after loading it:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte count
(14, 26)
```

Now we can query for nodes, links or logical expressions.

## Querying for nodes

We can search for a specific node:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_nodes --node-type Concept --node-name human
['af12f10f9ae2002a1607ba0b47ba8407']
```

**Important Note:** types and node names are case sensitive

The returned string `af12f10f9ae2002a1607ba0b47ba8407` is actually the handle of the Concept node named "human". You can use `--output-format` to see more human-friendly outputs.

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_nodes --node-type Concept --node-name human --output-format DICT
[{'handle': 'af12f10f9ae2002a1607ba0b47ba8407', 'type': 'Concept', 'name': 'human'}]
```

```
$ /scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_nodes --node-type Concept --node-name human --output-format JSON
[
    {
        "type": "Concept",
        "name": "human"
    }
]
```

We can search for all nodes of a given type:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_nodes --node-type Concept
['99d18c702e813b07260baf577c60c455', 'bdfe4e7a431f73386f37c6448afe5840', '80aff30094874e75028033a38ce677bb', 'b94941d8cd1c0ee4ad3dd3dcab52b964', 'b99ae727c787f1b13b452fd4c9ce1b9a', '08126b066d32ee37743e255a2558cccd', '5b34c54bee150c04f9fa584b899dc030', '0a32b476852eeb954979b87f5f6cb7af', 'bb34ce95f161a6b37ff54b3d4c817857', 'af12f10f9ae2002a1607ba0b47ba8407', '1cdffc6b0b89ff41d68bec237481d1e1', 'd03e59654221c1e8fcda404fd5c8d6cb', 'c1db9b517073e51eb7ef6fed608ec204', '4e8e26e3276af8a5c2ac2cc2dc95c6d2']
```

Again, we can use `--output-format` to see the output in other formats. E.g. in JSON:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_nodes --node-type Concept --output-format JSON
[
    {
        "type": "Concept",
        "name": "rhino"
    },
    {
        "type": "Concept",
        "name": "mammal"
    },
    {
        "type": "Concept",
        "name": "plant"
    },
    {
        "type": "Concept",
        "name": "vine"
    },
    {
        "type": "Concept",
        "name": "reptile"
    },
    {
        "type": "Concept",
        "name": "dinosaur"
    },
    {
        "type": "Concept",
        "name": "chimp"
    },
    {
        "type": "Concept",
        "name": "animal"
    },
    {
        "type": "Concept",
        "name": "earthworm"
    },
    {
        "type": "Concept",
        "name": "human"
    },
    {
        "type": "Concept",
        "name": "monkey"
    },
    {
        "type": "Concept",
        "name": "triceratops"
    },
    {
        "type": "Concept",
        "name": "snake"
    },
    {
        "type": "Concept",
        "name": "ent"
    }
]

```

Currently we don't allow queries for nodes using regexp in `--node-name` but this feature is alread queued :-)

## Querying for links

Links can be searched using link type, target types or target handles. Or any combination of them. To list all Similarity links between two Concept nodes:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_links --link-type Similarity --target-types "Concept,Concept"
['16f7e407087bfa0b35b13d13a1aadcae', '2a8a69c01305563932b957de4b3a9ba6', '2c927fdc6c0f1272ee439ceb76a6d1a4', '2d7abd27644a9c08a7ca2c8d68338579', '31535ddf214f5b239d3b517823cb8144', '72d0f9904bda2f89f9b68a1010ac61b5', '7ee00a03f67b39f620bd3d0f6ed0c3e6', '9923fc3e46d779c925d26ac4cf2d9e3b', 'a45af31b43ee5ea271214338a5a5bd61', 'abe6ad743fc81bd1c55ece2e1307a178', 'aef4d3da2565a640e15a52fd98d24d15', 'b5459e299a5c5e8662c427f7e01b3bf1', 'bad7472f41a0e7d601ca294eb4607c3a', 'e431a2eda773adf06ef3f9268f93deaf']
```

Wildcard can be used in the place of targets or target types. So we could have queries like:

```
./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_links --link-type Similarity --target-types "*,Concept"
./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_links --link-type Similarity --target-types "Concept,*"
./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_links --link-type Similarity --target-types "*,*"
```

In our rexamples, all those queries would return the same output.

These output are handles. We can inspect handles to get the actual atoms using the `atom` command:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte atom --output-format JSON --handle 16f7e407087bfa0b35b13d13a1aadcae
{
    "type": "Similarity",
    "targets": [
        {
            "type": "Concept",
            "name": "human"
        },
        {
            "type": "Concept",
            "name": "ent"
        }
    ]
}
```

We could search for all Similarity links pointing to Concept `human`:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte search_links --link-type Similarity --targets "af12f10f9ae2002a1607ba0b47ba8407,*" --output-format JSON
[
    {
        "type": "Similarity",
        "targets": [
            {
                "type": "Concept",
                "name": "monkey"
            },
            {
                "type": "Concept",
                "name": "human"
            }
        ]
    },
    {
        "type": "Similarity",
        "targets": [
            {
                "type": "Concept",
                "name": "chimp"
            },
            {
                "type": "Concept",
                "name": "human"
            }
        ]
    },
    {
        "type": "Similarity",
        "targets": [
            {
                "type": "Concept",
                "name": "ent"
            },
            {
                "type": "Concept",
                "name": "human"
            }
        ]
    }
]
```

## Querying for logical expressions

We can query for handles that satisfies a given set of constraints expressed as a logical combination of nodes and links. For instance, we could query for nodes $1, $2 and $3 such that there is an Inheritance link from $1 to $2 and another Inheritance link from $2 to $3:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte query --query "Link Inheritance \$1 \$2, Link Inheritance \$2 \$3, AND"
{{'$1': '5b34c54bee150c04f9fa584b899dc030', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, {'$1': '99d18c702e813b07260baf577c60c455', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, {'$1': 'd03e59654221c1e8fcda404fd5c8d6cb', '$2': '08126b066d32ee37743e255a2558cccd', '$3': 'b99ae727c787f1b13b452fd4c9ce1b9a'}, {'$1': '1cdffc6b0b89ff41d68bec237481d1e1', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, {'$1': 'af12f10f9ae2002a1607ba0b47ba8407', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, {'$1': '08126b066d32ee37743e255a2558cccd', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, {'$1': 'c1db9b517073e51eb7ef6fed608ec204', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}}
```

The output is a set of possible assignments for $1, $2 and $2. Let us re-format it to make it more clear:

```
{
    {'$1': '5b34c54bee150c04f9fa584b899dc030', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # chimp -> mammal -> animal
    {'$1': '99d18c702e813b07260baf577c60c455', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # rhino -> mammal -> animal
    {'$1': 'd03e59654221c1e8fcda404fd5c8d6cb', '$2': '08126b066d32ee37743e255a2558cccd', '$3': 'b99ae727c787f1b13b452fd4c9ce1b9a'}, # triceratops -> dinosaur -> reptile
    {'$1': '1cdffc6b0b89ff41d68bec237481d1e1', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # monkey -> mammal -> animal
    {'$1': 'af12f10f9ae2002a1607ba0b47ba8407', '$2': 'bdfe4e7a431f73386f37c6448afe5840', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # human -> mammal -> animal
    {'$1': '08126b066d32ee37743e255a2558cccd', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # dinosaur -> reptile -> animal
    {'$1': 'c1db9b517073e51eb7ef6fed608ec204', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}} # snake -> reptile -> animal
}
```

The query string is a list of terms separated by commas. Each term can be a node definition, a link definition or an operator. In this example we have two link definitions and the operator `AND`.

A link definition is the keyword `Link` followed by link type and the expected targets for that link. In this case we used variables as targets. Operators are pos-fixed `AND`, `OR` and `NOT`. `AND` and `OR` operates on any number of arguments (>= 1) while `NOT` operates on a single argument.

**Important Note:** keywords, types and node names are case sensitive

Lets redo the same query but excluding "mammals":

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte query --query "Node n1 Concept mammal, Link Inheritance \$1 n1, NOT, Link Inheritance \$1 \$2, Link Inheritance \$2 \$3, AND"
{
    {'$1': '08126b066d32ee37743e255a2558cccd', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # dinosaur -> reptile -> animal
    {'$1': 'c1db9b517073e51eb7ef6fed608ec204', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # snake -> reptile -> animal
    {'$1': 'd03e59654221c1e8fcda404fd5c8d6cb', '$2': '08126b066d32ee37743e255a2558cccd', '$3': 'b99ae727c787f1b13b452fd4c9ce1b9a'}, # triceratops -> dinosaur -> reptile
}
```

Nodes can be defined at the beginning of the query and used to build other query terms. They are not operated by operators and must be placed always as the first elements in a query, i.e. there can not be node definitions after the first `Link` term is defined.

The `NOT` operator will operate `Link Inheritance \$1 n1` and the `AND` operator will operate on the resulting negation and the two passed `Link` terms.

We could re-write this query like this, which will give the same output:

```
$ ./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte query --query "Node n1 Concept mammal, Link Inheritance \$1 \$2, Link Inheritance \$2 \$3, Link Inheritance \$1 n1, NOT, AND"
{
    {'$1': '08126b066d32ee37743e255a2558cccd', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # dinosaur -> reptile -> animal
    {'$1': 'c1db9b517073e51eb7ef6fed608ec204', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # snake -> reptile -> animal
    {'$1': 'd03e59654221c1e8fcda404fd5c8d6cb', '$2': '08126b066d32ee37743e255a2558cccd', '$3': 'b99ae727c787f1b13b452fd4c9ce1b9a'}, # triceratops -> dinosaur -> reptile
}
```

Now lets redo this query but adding any "Inheritance" links pointing to "human"

```
./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte query --query "Node n1 Concept mammal, Node n2 Concept human, Link Inheritance \$1 \$2, Link Inheritance \$2 \$3, Link Inheritance \$1 n1, NOT, AND, Link Inheritance n2 \$2, OR"
{
    {'$1': '08126b066d32ee37743e255a2558cccd', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # dinosaur -> reptile -> animal
    {'$1': 'c1db9b517073e51eb7ef6fed608ec204', '$2': 'b99ae727c787f1b13b452fd4c9ce1b9a', '$3': '0a32b476852eeb954979b87f5f6cb7af'}, # snake -> reptile -> animal
    {'$1': 'd03e59654221c1e8fcda404fd5c8d6cb', '$2': '08126b066d32ee37743e255a2558cccd', '$3': 'b99ae727c787f1b13b452fd4c9ce1b9a'}, # triceratops -> dinosaur -> reptile
    {'$2': 'bdfe4e7a431f73386f37c6448afe5840'} # mammal
}
```

The answer for this query is the answer for the previous one plus a solution where `$2 == mammal` and the other variables don't care.

Lets do another query example with `OR`. Now we'll search for any concepts that are similar to humans or snakes.

```
./scripts/das-cli.sh --das-key zkgftedbgvlwstjivhte query --query "Node n1 Concept human, Node n2 Concept snake, Link Similarity \$1 n1, Link Similarity \$1 n2, OR"
{
    *{'$1': '5b34c54bee150c04f9fa584b899dc030'}, # chimp
    *{'$1': '1cdffc6b0b89ff41d68bec237481d1e1'}, # monkey
    *{'$1': 'b94941d8cd1c0ee4ad3dd3dcab52b964'}, # vine
    *{'$1': '4e8e26e3276af8a5c2ac2cc2dc95c6d2'}, # ent
    *{'$1': 'bb34ce95f161a6b37ff54b3d4c817857'}  # earthworm
}
```

The asterisk `*` in the beginning of each assignment indicates an unordered assignment. This is not relevant for assignments with only one variable as the above but it's relevant when we have more than one variable. Let's search for concepts that are similar to each other:

```
./scripts/das-cli.sh --das-key iemxjwpwmkoildfjwecj query --query "Link Similarity \$1 \$2"
{
    *{'$1': 'af12f10f9ae2002a1607ba0b47ba8407', '$2': '4e8e26e3276af8a5c2ac2cc2dc95c6d2'}, # human <-> ent
    *{'$1': 'bb34ce95f161a6b37ff54b3d4c817857', '$2': 'c1db9b517073e51eb7ef6fed608ec204'}, # earthworm <-> snake
    *{'$1': 'b94941d8cd1c0ee4ad3dd3dcab52b964', '$2': 'c1db9b517073e51eb7ef6fed608ec204'}, # vine <-> snake
    *{'$1': '1cdffc6b0b89ff41d68bec237481d1e1', '$2': 'af12f10f9ae2002a1607ba0b47ba8407'}, # monkey <-> human
    *{'$1': '99d18c702e813b07260baf577c60c455', '$2': 'd03e59654221c1e8fcda404fd5c8d6cb'}, # rhino <-> triceratops
    *{'$1': '1cdffc6b0b89ff41d68bec237481d1e1', '$2': '5b34c54bee150c04f9fa584b899dc030'}, # monkey <-> chimp
    *{'$1': '5b34c54bee150c04f9fa584b899dc030', '$2': 'af12f10f9ae2002a1607ba0b47ba8407'}  # chimp <-> human
}
```

In these assignments, the values for $1 and $2 are interchangeable.

# How to build and run a server

In this tutorial we show how to build and deploy a DAS gRPC server using Docker containers ([Docker documentation](https://docs.docker.com/)).

We tested the tutorial in a Ubuntu 22.04 but it should work in previous versions of Ubuntu as well. If you tested it in another Ubuntu version or another OS and needed to make adjustment, please submit a PR so we can update this documentation.

## Step 1 - download Github repository

```
$ git clone git@github.com:singnet/das.git
```

## Step 2 - build service image

There's a Bash script to build the service image.

```
$ cd das
$ ./scripts/build-service.sh
``` 

To be sure this step worked, you can check up the docker image using:

```
$ docker image list
```

You are supposed to see an entry like this:

```
das_service   latest    fb5f0224202f   2 minutes ago   1.95GB
```

## Step 3 - build extra docker images and start the service

Prepare environment, by exporting all necessary variables:
```
$ source environment
```

There are two other Docker images to build but this can be done while we put the service container up. Just call the following:

```
$ ./scripts/service-up.sh
```

After building the images, it's supposed to output something like this:

```
Creating das-mongo-1     ... done
Creating das-couchbase-1 ... done
Creating das-das_service-1 ... done
INFO: Waiting for Couchbase...
SUCCESS: Cluster initialized
SUCCESS: Couchbase is ready.
```

At this point you should have three Docker containers running. You can inspect then if you want:

```
$ docker ps
CONTAINER ID   IMAGE         COMMAND                  CREATED         STATUS         PORTS                                                                                                                                                           NAMES
11fd5d80265c   das_service   "python3 service/ser…"   2 minutes ago   Up 2 minutes                                                                                                                                                                   das-das_service-1
bf299b85e372   mongo         "docker-entrypoint.s…"   2 minutes ago   Up 2 minutes   0.0.0.0:27017->27017/tcp, :::27017->27017/tcp                                                                                                                   das-mongo-1
3d79b349fd60   couchbase     "/entrypoint.sh couc…"   2 minutes ago   Up 2 minutes   8096/tcp, 0.0.0.0:8091-8095->8091-8095/tcp, :::8091-8095->8091-8095/tcp, 11207/tcp, 11211/tcp, 0.0.0.0:11210->11210/tcp, :::11210->11210/tcp, 18091-18096/tcp   das-couchbase-1
```

You are ready to go! At this point you are ready to submit requests to your gRPC server. 
