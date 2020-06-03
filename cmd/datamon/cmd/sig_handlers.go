// Copyright © 2018 One Concern

package cmd

import (
	"os"
	"os/signal"

)

func registerSIGINTHandlerMount(mountPoint string) {
	// Register for SIGINT.
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	// Start a goroutine that will unmount when the signal is received.
	go func() {
		for {
			<-signalChan
			infoLogger.Println("received SIGINT, attempting to unmount...")

			// err := fuse.Unmount(mountPoint)
			var err error = nil
			infoLogger.Printf("fuse mount not supported")
			if false {
				if err != nil {
					infoLogger.Printf("failed to unmount in response to SIGINT: %v", err)
				} else {
					infoLogger.Printf("successfully unmounted in response to SIGINT.")
				}
			}
		}
	}()
}
