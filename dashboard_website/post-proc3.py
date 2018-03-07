def func(result):
	(x_vals,y_vals) = ([],[])
	print(result)
	result.sort(key=lambda item:item["count(t)"], reverse=True)
	for i in range(10):
		x_vals.append(result[i]["u"]["id"])
		y_vals.append(result[i]["count(t)"])
		print(result[i]["u"]["id"], result[i]["count(t)"])
	print(x_vals,y_vals)
	return (x_vals,y_vals)

