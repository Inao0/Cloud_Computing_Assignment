sortStreaming:
	sort --merge Result/Stateful-Streaming/results_partition_* -o Result/Stateful-Streaming/result
	sort Result/Stateful-Streaming/result -o Result/Stateful-Streaming_sorted_result.txt

sortDataset:
	sort --merge Result/Dataset/*.csv -o Result/Dataset/result.csv
	sort Result/Dataset/result.csv -o Result/Dataset_sorted_result.csv

sortBatch :
	sort Result/Batch.txt -o Result/sorted_Batch.txt

cleanResults :
	rm -rf Result

