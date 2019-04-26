// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var (
	memberPeerURLs string
	isLearner      bool
)

// NewMemberCommand returns the cobra command for "member".
func NewMemberCommand() *cobra.Command {
	mc := &cobra.Command{
		Use:   "member <subcommand>",
		Short: "Membership related commands",
	}

	mc.AddCommand(NewMemberAddCommand())
	mc.AddCommand(NewMemberRemoveCommand())
	mc.AddCommand(NewMemberUpdateCommand())
	mc.AddCommand(NewMemberListCommand())
	mc.AddCommand(NewMemberPromoteCommand())

	return mc
}

// NewMemberAddCommand returns the cobra command for "member add".
func NewMemberAddCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "add <memberName> [options]",
		Short: "Adds a member into the cluster",

		Run: memberAddCommandFunc,
	}

	cc.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs for the new member.")
	cc.Flags().BoolVar(&isLearner, "learner", false, "indicates if the new member is raft learner")

	return cc
}

// NewMemberRemoveCommand returns the cobra command for "member remove".
func NewMemberRemoveCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "remove <memberID>",
		Short: "Removes a member from the cluster",

		Run: memberRemoveCommandFunc,
	}

	return cc
}

// NewMemberUpdateCommand returns the cobra command for "member update".
func NewMemberUpdateCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "update <memberID> [options]",
		Short: "Updates a member in the cluster",

		Run: memberUpdateCommandFunc,
	}

	cc.Flags().StringVar(&memberPeerURLs, "peer-urls", "", "comma separated peer URLs for the updated member.")

	return cc
}

// NewMemberListCommand returns the cobra command for "member list".
func NewMemberListCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "list",
		Short: "Lists all members in the cluster",
		Long: `When --write-out is set to simple, this command prints out comma-separated member lists for each endpoint.
The items in the lists are ID, Status, Name, Peer Addrs, Client Addrs, Is Learner.
`,

		Run: memberListCommandFunc,
	}

	return cc
}

// NewMemberPromoteCommand returns the cobra command for "member promote".
func NewMemberPromoteCommand() *cobra.Command {
	cc := &cobra.Command{
		Use:   "promote <memberID>",
		Short: "Promotes a non-voting member in the cluster",
		Long: `Promotes a non-voting learner member to a voting one in the cluster.
`,

		Run: memberPromoteCommandFunc,
	}

	return cc
}

// memberAddCommandFunc executes the "member add" command.
func memberAddCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 1 {
		ExitWithError(ExitBadArgs, errors.New("member name not provided"))
	}
	if len(args) > 1 {
		ev := "too many arguments"
		for _, s := range args {
			if strings.HasPrefix(strings.ToLower(s), "http") {
				ev += fmt.Sprintf(`, did you mean --peer-urls=%s`, s)
			}
		}
		ExitWithError(ExitBadArgs, errors.New(ev))
	}
	newMemberName := args[0]

	if len(memberPeerURLs) == 0 {
		ExitWithError(ExitBadArgs, errors.New("member peer urls not provided"))
	}

	urls := strings.Split(memberPeerURLs, ",")
	ctx, cancel := commandCtx(cmd)
	cli := mustClientFromCmd(cmd)
	resp, err := cli.MemberAdd(ctx, urls, isLearner)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	newID := resp.Member.ID

	display.MemberAdd(*resp)

	if _, ok := (display).(*simplePrinter); ok {
		ctx, cancel = commandCtx(cmd)
		listResp, err := cli.MemberList(ctx)
		// get latest member list; if there's failover new member might have outdated list
		for {
			if err != nil {
				ExitWithError(ExitError, err)
			}
			if listResp.Header.MemberId == resp.Header.MemberId {
				break
			}
			// quorum get to sync cluster list
			gresp, gerr := cli.Get(ctx, "_")
			if gerr != nil {
				ExitWithError(ExitError, err)
			}
			resp.Header.MemberId = gresp.Header.MemberId
			listResp, err = cli.MemberList(ctx)
		}
		cancel()

		conf := []string{}
		for _, memb := range listResp.Members {
			for _, u := range memb.PeerURLs {
				n := memb.Name
				if memb.ID == newID {
					n = newMemberName
				}
				conf = append(conf, fmt.Sprintf("%s=%s", n, u))
			}
		}

		fmt.Print("\n")
		fmt.Printf("ETCD_NAME=%q\n", newMemberName)
		fmt.Printf("ETCD_INITIAL_CLUSTER=%q\n", strings.Join(conf, ","))
		fmt.Printf("ETCD_INITIAL_ADVERTISE_PEER_URLS=%q\n", memberPeerURLs)
		fmt.Printf("ETCD_INITIAL_CLUSTER_STATE=\"existing\"\n")
	}
}

// memberRemoveCommandFunc executes the "member remove" command.
func memberRemoveCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("member ID is not provided"))
	}

	id, err := strconv.ParseUint(args[0], 16, 64)
	if err != nil {
		ExitWithError(ExitBadArgs, fmt.Errorf("bad member ID arg (%v), expecting ID in Hex", err))
	}

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).MemberRemove(ctx, id)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	display.MemberRemove(id, *resp)
}

// memberUpdateCommandFunc executes the "member update" command.
func memberUpdateCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("member ID is not provided"))
	}

	id, err := strconv.ParseUint(args[0], 16, 64)
	if err != nil {
		ExitWithError(ExitBadArgs, fmt.Errorf("bad member ID arg (%v), expecting ID in Hex", err))
	}

	if len(memberPeerURLs) == 0 {
		ExitWithError(ExitBadArgs, fmt.Errorf("member peer urls not provided"))
	}

	urls := strings.Split(memberPeerURLs, ",")

	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).MemberUpdate(ctx, id, urls)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}

	display.MemberUpdate(id, *resp)
}

// memberListCommandFunc executes the "member list" command.
func memberListCommandFunc(cmd *cobra.Command, args []string) {
	ctx, cancel := commandCtx(cmd)
	resp, err := mustClientFromCmd(cmd).MemberList(ctx)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}

	display.MemberList(*resp)
}

// memberPromoteCommandFunc executes the "member promote" command.
func memberPromoteCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		ExitWithError(ExitBadArgs, fmt.Errorf("member ID is not provided"))
	}

	id, err := strconv.ParseUint(args[0], 16, 64)
	if err != nil {
		ExitWithError(ExitBadArgs, fmt.Errorf("bad member ID arg (%v), expecting ID in Hex", err))
	}

	ctx, cancel := commandCtx(cmd)
	eps, err := endpointsFromCmd(cmd)
	if err != nil {
		ExitWithError(ExitError, err)
	}

	var (
		leaderEp    string
		leaderFound bool
	)
	for _, ep := range eps {
		cfg := clientConfigFromCmd(cmd)
		cfg.endpoints = []string{ep}
		cli := cfg.mustClient()
		resp, serr := cli.Status(ctx, ep)
		if serr != nil {
			ExitWithError(ExitError, serr)
		}
		if resp.Header.GetMemberId() == resp.Leader {
			leaderFound = true
			leaderEp = ep
			break
		}
		cli.Close()
	}
	if !leaderFound {
		ExitWithError(ExitBadArgs, fmt.Errorf("no leader endpoint found in %v", eps))
	}

	cfg := clientConfigFromCmd(cmd)
	cfg.endpoints = []string{leaderEp}
	resp, err := cfg.mustClient().MemberPromote(ctx, id)
	cancel()
	if err != nil {
		ExitWithError(ExitError, err)
	}
	display.MemberPromote(id, *resp)
}
