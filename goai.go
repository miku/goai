// OAI key concepts
// resource, item, record
//
// resource: what the data is about
// item: repository constituent (container, possibly multiple formats)
// record: metadata in a specific format (XML)
//
// A record is unabiguously defined by:
// 1) the unique identifier of the item
// 2) the `metadataPrefix` identifying the metadata format
// 3) the datestamp of the record
//
// The XML contains three sections: header, metadata and about.
//
// A repository must declare one of three ways to handle deletions:
// no, persistent, transient (most thourough is persistent).
//
// From the Spec:
// If a repository does not keep track of deletions then such records will
// simply vanish from responses and there will be no way for a harvester
// to discover deletions through continued incremental harvesting.
//
// A set it used for selective harvesting.

package main

import (
    "fmt"
    "github.com/codegangsta/cli"
    "gopkg.in/xmlpath.v1"
    "io/ioutil"
    "log"
    "net/http"
    "net/url"
    "os"
    "strings"
)

type Repository struct {
    Url url.URL
}

func BodyAsString(locator url.URL) (string, error) {
    log.Println(locator.String())
    resp, err := http.Get(locator.String())
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()
    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        return "", err
    }
    return string(body), nil
}

func ParseResumptionToken(s string) string {
    reader := strings.NewReader(s)
    path := xmlpath.MustCompile("OAI-PMH/ListIdentifiers/resumptionToken")
    root, err := xmlpath.Parse(reader)
    if err != nil {
        log.Fatal(err)
    }
    if value, ok := path.String(root); ok {
        return value
    }
    return ""
}

func (r Repository) GetRecord(identifier string, metadataPrefix string) (string, error) {
    Url := r.Url
    parameters := url.Values{}
    parameters.Add("verb", "GetRecord")
    parameters.Add("identifier", identifier)
    parameters.Add("metadataPrefix", metadataPrefix)
    Url.RawQuery = parameters.Encode()
    return BodyAsString(Url)
}

func (r Repository) Identify() (string, error) {
    Url := r.Url
    parameters := url.Values{}
    parameters.Add("verb", "Identify")
    Url.RawQuery = parameters.Encode()
    return BodyAsString(Url)
}

func (r Repository) ListIdentifiers(from string, until string, metadataPrefix string, set string, resumptionToken string) (string, error) {
    Url := r.Url
    parameters := url.Values{}
    parameters.Add("verb", "ListIdentifiers")

    // resumptionToken is an exclusive argument, if it is present prefer it
    if len(resumptionToken) > 0 {
        parameters.Add("resumptionToken", resumptionToken)
    } else {
        if len(from) > 0 {
            parameters.Add("from", from)
        }
        if len(until) > 0 {
            parameters.Add("until", until)
        }
        if len(set) > 0 {
            parameters.Add("set", set)
        }
        if len(metadataPrefix) > 0 {
            parameters.Add("metadataPrefix", metadataPrefix)
        }
    }
    Url.RawQuery = parameters.Encode()
    body, err := BodyAsString(Url)
    log.Println(ParseResumptionToken(body))
    return body, err
}

func main() {
    app := cli.NewApp()

    app.Commands = []cli.Command{
        {
            Name:      "GetRecord",
            ShortName: "get",
            Usage:     "Get a single record from the repository.",
            Action: func(c *cli.Context) {
                repoUrl, err := url.Parse(c.String("url"))
                if err != nil {
                    log.Println(err)
                    os.Exit(1)
                }
                repo := Repository{Url: *repoUrl}
                result, err := repo.GetRecord(c.String("id"), c.String("prefix"))
                if err != nil {
                    log.Println(err)
                    os.Exit(1)
                } else {
                    fmt.Print(result)
                }
            },
            Flags: []cli.Flag{
                cli.StringFlag{"url, u", "http://arXiv.org/oai2", "repository URL"},
                cli.StringFlag{"id, i", "oai:arXiv.org:cs/0112017", "identifier"},
                cli.StringFlag{"prefix, p", "oai_dc", "metadataPrefix"},
            },
        },
        {
            Name:      "Identify",
            ShortName: "id",
            Usage:     "Retrieve information about a repository.",
            Action: func(c *cli.Context) {
                repoUrl, err := url.Parse(c.String("url"))
                if err != nil {
                    log.Println(err)
                    os.Exit(1)
                }
                repo := Repository{Url: *repoUrl}
                result, err := repo.Identify()
                if err != nil {
                    log.Println(err)
                    os.Exit(1)
                } else {
                    fmt.Print(result)
                }
            },
            Flags: []cli.Flag{
                cli.StringFlag{"url, u", "http://arXiv.org/oai2", "repository URL"},
            },
        },
        {
            Name:      "ListIdentifiers",
            ShortName: "ls",
            Usage:     "Retrieve the identifiers from a repository.",
            Action: func(c *cli.Context) {
                repoUrl, err := url.Parse(c.String("url"))
                if err != nil {
                    log.Println(err)
                    os.Exit(1)
                }
                repo := Repository{Url: *repoUrl}
                result, err := repo.ListIdentifiers(c.String("from"), c.String("until"), c.String("prefix"), c.String("set"), c.String("token"))
                if err != nil {
                    log.Println(err)
                    os.Exit(1)
                } else {
                    fmt.Print(result)
                }
            },
            Flags: []cli.Flag{
                cli.StringFlag{"url, u", "http://arXiv.org/oai2", "repository URL"},
                cli.StringFlag{"from, f", "", "earliest date"},
                cli.StringFlag{"until, t", "", "latest date"},
                cli.StringFlag{"prefix, p", "oai_dc", "metadataPrefix"},
                cli.StringFlag{"token, r", "", "resumptionToken"},
                cli.StringFlag{"set, s", "", "set name"},
            },
        },
    }

    app.Name = "goai"
    app.Version = "0.1.0"
    app.Usage = "OAI command line client"
    app.Run(os.Args)
}
