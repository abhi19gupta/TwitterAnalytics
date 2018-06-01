def func(inputs):
	inputs = list(zip(inputs["uid"], inputs["count"]))
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