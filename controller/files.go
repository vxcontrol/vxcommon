package controller

// sFiles is universal container for modules files loader
type sFiles struct {
	flt tFilesLoaderType
	IFilesLoader
}

// NewFilesFromS3 is function which constructed Files loader object
func NewFilesFromS3() (IFilesLoader, error) {
	return &sFiles{
		flt:          eS3FilesLoader,
		IFilesLoader: &filesLoaderS3{},
	}, nil
}

// NewFilesFromFS is function which constructed Files loader object
func NewFilesFromFS(path string) (IFilesLoader, error) {
	return &sFiles{
		flt:          eFSFilesLoader,
		IFilesLoader: &filesLoaderFS{path: path},
	}, nil
}
