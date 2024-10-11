package main

import (
	"fmt"
	"log"
	"net/http"
	"sync/atomic"

	"github.com/spf13/cobra"
)

////////////////////////////////////////////////////////////////////////////////

func main() {
	rootCmd := &cobra.Command{}

	var imageFilePath string
	var otherImageFilePath string
	var port int

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start image file server",
		Run: func(cmd *cobra.Command, args []string) {
			var useOtherImage atomic.Bool

			http.HandleFunc("/",
				func(w http.ResponseWriter, r *http.Request) {
					var filePath string
					if useOtherImage.Load() {
						filePath = otherImageFilePath
					} else {
						filePath = imageFilePath
					}
					w.Header().Set("ETag", filePath)

					http.ServeFile(w, r, filePath)
					log.Printf(
						"Served request %+v from file %v, response header %+v",
						r,
						filePath,
						w.Header(),
					)
				},
			)

			http.HandleFunc("/use_other_image",
				func(w http.ResponseWriter, r *http.Request) {
					log.Printf("Using other image file %v", otherImageFilePath)
					useOtherImage.Store(true)
				},
			)

			http.HandleFunc("/use_default_image",
				func(w http.ResponseWriter, r *http.Request) {
					log.Printf("Using default image file %v", imageFilePath)
					useOtherImage.Store(false)
				},
			)

			endpoint := fmt.Sprintf(":%v", port)
			log.Printf(
				"Listening on %v, serving files %v %v\n",
				endpoint,
				imageFilePath,
				otherImageFilePath,
			)

			log.Fatal(http.ListenAndServe(endpoint, nil))
		},
	}

	startCmd.Flags().StringVar(
		&imageFilePath,
		"image-file-path",
		"",
		"Path to the image file",
	)
	if err := startCmd.MarkFlagRequired("image-file-path"); err != nil {
		log.Fatalf("Error setting flag image-file-path as required: %v", err)
	}

	startCmd.Flags().StringVar(
		&otherImageFilePath,
		"other-image-file-path",
		"",
		"Path to the additional image file, you can start using it by calling /use_other_image handler",
	)

	startCmd.Flags().IntVar(
		&port,
		"port",
		8080,
		"Http port to listen on",
	)

	rootCmd.AddCommand(startCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Failed to run: %v", err)
	}
}
