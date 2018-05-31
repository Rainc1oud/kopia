package cli

import (
	"context"
	"fmt"
	"sort"

	"github.com/kopia/kopia/block"
	"github.com/kopia/kopia/repo"
)

var (
	blockListCommand        = blockCommands.Command("list", "List blocks").Alias("ls")
	blockListLong           = blockListCommand.Flag("long", "Long output").Short('l').Bool()
	blockListPrefix         = blockListCommand.Flag("prefix", "Prefix").String()
	blockListIncludeDeleted = blockListCommand.Flag("deleted", "Include deleted blocks").Bool()
	blockListSort           = blockListCommand.Flag("sort", "Sort order").Default("name").Enum("name", "size", "time", "none", "pack")
	blockListReverse        = blockListCommand.Flag("reverse", "Reverse sort").Short('r').Bool()
	blockListSummary        = blockListCommand.Flag("summary", "Summarize the list").Short('s').Bool()
)

func runListBlocksAction(ctx context.Context, rep *repo.Repository) error {
	blocks, err := rep.Blocks.ListBlockInfos(*blockListPrefix, *blockListIncludeDeleted)
	if err != nil {
		return err
	}

	sortBlocks(blocks)

	var count int
	var totalSize int64
	uniquePacks := map[block.PhysicalBlockID]bool{}
	for _, b := range blocks {
		totalSize += int64(b.Length)
		count++
		if b.PackBlockID != "" {
			uniquePacks[b.PackBlockID] = true
		}
		if *blockListLong {
			optionalDeleted := ""
			if b.Deleted {
				optionalDeleted = " (deleted)"
			}
			fmt.Printf("%v %v %v %v+%v%v\n", b.BlockID, b.Timestamp().Format(timeFormat), b.PackBlockID, b.PackOffset, b.Length, optionalDeleted)
		} else {
			fmt.Printf("%v\n", b.BlockID)
		}
	}

	if *blockListSummary {
		fmt.Printf("Total: %v blocks, %v packs, %v bytes\n", count, len(uniquePacks), totalSize)
	}

	return nil
}

func sortBlocks(blocks []block.Info) {
	maybeReverse := func(b bool) bool { return b }

	if *blockListReverse {
		maybeReverse = func(b bool) bool { return !b }
	}

	switch *blockListSort {
	case "name":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].BlockID < blocks[j].BlockID) })
	case "size":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].Length < blocks[j].Length) })
	case "time":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(blocks[i].TimestampSeconds < blocks[j].TimestampSeconds) })
	case "pack":
		sort.Slice(blocks, func(i, j int) bool { return maybeReverse(comparePacks(blocks[i], blocks[j])) })
	}
}

func comparePacks(a, b block.Info) bool {
	if a, b := a.PackBlockID, b.PackBlockID; a != b {
		return a < b
	}

	return a.PackOffset < b.PackOffset
}

func init() {
	blockListCommand.Action(repositoryAction(runListBlocksAction))
}
