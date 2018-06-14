About postprocessing functions
====================================

Need of post processing function
-----------------------------------

Some processing can be done in a cypher query in case of neo4j, and further in case of mongoDB, there is functionality to write custom functions to be included in the aggregation pipeline. But we provide the user the ability to create post processing function. The major reasons behind this are:

    *  It may be easy to do some projection on data output by a query post the execution, rather than coding it in the cypher in case of neo4j, or the aggregation pipeline in case of mongoDB.
    * On similar lines as above, the user may need to aggregate multiple outputs from different queries in a postprocessing function in a custom manner not supported by the query mechanism of the databases.

Format of post processing functions
--------------------------------------
We treat the postprocessing functions as queries. The idea behind treating post processing functions as a query is to provide simplistic abstraction while creating a DAG. Thus while creating a DAG, a user has to just compose queries which can either be query to any of the two databases or a post processing function as well. Thus, given the DAG abstraction, the user can feed the output of the query(ies) into a postprocessing node.

Further to support this abstraction, we require the post processing function to accept a dictionary of lists of native python objects(named "inputs") and return a dicionary(named "ret") in same format. The function should further be named as "func". This requires that the user specifies the input and output varibale names while creating the post processing function. This will be expalined in detail in the DAG section.

Another way(instead of asking the user to explicitly provide the input and output variable names) in which post processing function could have been created is to just take as input the code of the function, parse it to get the number of inputs and their names. This is relatively easy. But, the issue is to get the output varibales. This is a difficult problem and exactly this is used to generate automatic documentation of python code. But has been observed, even it misses the names of return varibales. Its easy in case named varibales are returned but the issue is when epressions are returned(for example the code contains ``return l[:10]``, its not clear what should be the name of the return variable). Thus, out adopted method of dealing with dictionaries with named variables provides a clean abstraction over the the alternatve.

Here is an example of a post processing function to output the union of lists input to it:

.. code-block:: python

    def func(input):
        """
        Function to take union of two lists.
        :param: input - a dictionry with the attribute names as keys and their values as dict-values.
        :return: a dictionary with output variables as keys.
        """
        l1 = input["list1"]
        l2 = input["list2"]
        for x in l2:
            if x not in l1:
                l1.append(x)

        ret = {}
        ret["l_out"] = l1
        return ret


Post processing functions are also used to display custom metric. To view a custom metric, the user is required to specify a post processing function which accepts as inputs the outputs of any of the queries in the DAG and outputs a x and y coordinates to be used for plotting.

Here is another example of a post processing function to create a custom metric to plot the users with their number of tweets:

.. code-block:: python

    def func(inputs):
        inputs = list(zip(inputs["userid"], inputs["count"]))
        inputs.sort(key=lambda item:item[1], reverse=True)
        x_vals = []
        y_vals = []
        for i in range(10):
            x_vals.append(str(inputs[i][0]))
            y_vals.append(inputs[i][1])

        ret = {}
        ret["x_vals"] = x_vals
        ret["y_vals"] = y_vals
        return ret


Executing post processing function
--------------------------------------
To execute the post processing function, we just provide include the inputs in the context being passed to the function. The execution requires these three steps:

    * Compilation : Any code errors are output to the user at this point.
    * Passing the inputs to the function and executing its code.
    * Obtaining the outputs : The output ret dictionary is pushed on the context by the function.

This can be seen in this code snippet used to execute the post processing functions:

.. code-block:: python

    context = {"inputs":copy.deepcopy(inputs)}
    try:
        compile(function_code,'','exec')
        exec(functiona_code + "\n" + "ret = func(inputs)", context)
        for out in outputs:
            ret[out] = context[out]
    except Exception as e:
        print("Exeption while executing Post proc function: %s, %s"%(type(e),e))
