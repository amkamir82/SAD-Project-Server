from Codes.SADProject.Broker.file.write import Write

writer = Write('3')
writer.write_data('id1', "id".encode())
writer.write_data('id1', "hi".encode())
writer.write_data('id1', "koft".encode())
