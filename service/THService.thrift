service THService {
    string Echo()
    string Exec(1:string command)
    string FileTransfer(1:string filename, 2:binary content)
}