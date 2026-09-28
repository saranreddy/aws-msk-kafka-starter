"""Architecture diagram for saranreddy/aws-msk-kafka-starter.

Verified against main @ f72c0d0. Every node maps to infra/*.tf or scripts/*.py.

Render:  pip install diagrams   (also needs Graphviz: apt install graphviz / brew install graphviz)
         python docs/architecture.py   ->  docs/architecture.png (written next to this script)
"""
import os

from diagrams import Cluster, Diagram, Edge, getdiagram
from diagrams.aws.analytics import ManagedStreamingForKafka
from diagrams.aws.compute import EC2
from diagrams.aws.general import InternetAlt1, User
from diagrams.aws.management import CloudwatchLogs
from diagrams.aws.network import InternetGateway, NATGateway
from diagrams.aws.security import IAMRole
from diagrams.onprem.iac import Terraform
from diagrams.onprem.queue import Kafka
from diagrams.programming.language import Python

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "architecture")  # -> architecture.png next to this script

FONT = "DejaVu Sans"
GRAPH = {
    "fontname": FONT, "fontsize": "34", "labelloc": "t", "pad": "0.4",
    "nodesep": "0.4", "ranksep": "1.0", "splines": "spline", "newrank": "true",
    "compound": "true",
}
NODE = {"fontname": FONT, "fontsize": "21", "imagepos": "tc"}
EDGE = {"fontname": FONT, "fontsize": "19", "color": "#555555",
        # enter/leave icons at mid-height so arrowheads never land on label text
        "tailport": "e", "headport": "w"}

# diagrams.Edge hard-codes a 13pt label font on every edge; raise it so edge labels stay
# readable when the PNG is scaled down to README width.
Edge._default_edge_attrs = {"fontcolor": "#2D3436", "fontname": FONT, "fontsize": "19"}


def box(bg, pen, style="rounded"):
    return {"bgcolor": bg, "pencolor": pen, "fontname": FONT, "fontsize": "21",
            "style": style, "labeljust": "l", "margin": "24"}


TF_BOX = box("#fff4e0", "#e66100")                  # deployed by Terraform
SUB_BOX = box("#fffaf2", "#e66100")                 # sub-group inside a Terraform box
RUN_BOX = box("#e8f1fb", "#1a5fb4")                 # created by scripts / CLI
EXEC_BOX = box("#f3eefa", "#613583")                # per execution / runtime
MANAGED_BOX = box("#f6f5f4", "#9a9996", "dashed")   # not created by this repo
ACCOUNT_BOX = box("#ffffff", "#232f3e")

FLOW = dict(color="#1a5fb4", fontcolor="#1a5fb4", penwidth="2.2")
IO = dict(color="#26a269", fontcolor="#1e7d4f", penwidth="1.8")
IAM = dict(color="#c01c28", fontcolor="#c01c28", style="dashed", penwidth="1.6", constraint="false")
AUX = dict(color="#8a8a8a", fontcolor="#5e5c64", style="dotted", penwidth="1.8")
SETUP = dict(color="#e66100", fontcolor="#c64600", style="dashed", penwidth="1.8")
MANUAL = dict(color="#26a269", fontcolor="#1e7d4f", style="dashed", penwidth="2.2")
FAIL = dict(color="#c01c28", fontcolor="#c01c28", penwidth="2.2")
OPT = dict(color="#b5835a", fontcolor="#8f5f3a", style="dashed", penwidth="1.8")
HIDDEN = dict(style="invis")
DOWN = dict(tailport="s", headport="n")
UP = dict(tailport="n", headport="s")


def same_rank(*nodes):
    getdiagram().dot.body.append("{rank=same; " + " ".join(f'"{n._id}";' for n in nodes) + "}")


GRAPH["pad"] = "0.7"

with Diagram(
    "aws-msk-kafka-starter",
    filename=OUT, outformat="png", show=False, direction="LR",
    graph_attr=GRAPH, node_attr=NODE, edge_attr=EDGE,
):
    eng = User("Platform\nengineer")
    tf = Terraform("terraform apply\n(infra/)")

    with Cluster("AWS account  (default us-east-1)", graph_attr=ACCOUNT_BOX):
        with Cluster("Deployed by Terraform", graph_attr=TF_BOX):
            role = IAMRole("MSK client role\n+ instance profile\n(trusts EC2 and\nthe deployer)")
            loggroup = CloudwatchLogs("Log group\n/aws/msk/<name>\n7-day retention\n(not attached\nto the cluster)")

        with Cluster("VPC 10.0.0.0/16  (Terraform, 2 AZs)", graph_attr=TF_BOX):
            with Cluster("You provide - not in Terraform", graph_attr=MANAGED_BOX):
                host = EC2("Client host\nin the VPC\n(attach client-sg)")
                scripts = Python("create_topic.py\nproduce.py\nconsume.py\nsmoke_test.py")

            with Cluster("Private subnets x2  (msk-sg)", graph_attr=SUB_BOX) as priv:
                msk = ManagedStreamingForKafka("MSK Serverless\nIAM auth only")
                topic = Kafka("demo-topic\n2 partitions\n(script-created)")

            with Cluster("Public subnets x2", graph_attr=SUB_BOX):
                nat = NATGateway("NAT gateway\n1 shared (default)\nor 1 per AZ")
                igw = InternetGateway("Internet\ngateway")

    internet = InternetAlt1("Internet")

    eng >> Edge(label="deploy", **DOWN, **SETUP) >> tf
    same_rank(eng, tf)
    tf >> Edge(**SETUP) >> role
    tf >> Edge(**SETUP) >> loggroup
    eng >> Edge(label="runs", **FLOW) >> host
    host >> Edge(**FLOW) >> scripts
    role >> Edge(tailport="n", headport="s", **IAM) >> host
    scripts >> Edge(label="SASL_SSL\nAWS_MSK_IAM\n:9098", **FLOW) >> msk
    msk >> Edge(label="hosts", **FLOW) >> topic
    topic >> Edge(label="private route\n0.0.0.0/0", ltail=priv.name, **AUX) >> nat
    nat >> Edge(**AUX) >> igw
    igw >> Edge(**AUX) >> internet
