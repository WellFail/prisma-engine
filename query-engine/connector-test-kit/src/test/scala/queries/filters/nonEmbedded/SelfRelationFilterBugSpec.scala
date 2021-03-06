package queries.filters.nonEmbedded

import org.scalatest.{FlatSpec, Matchers}
import util.ConnectorCapability.JoinRelationLinksCapability
import util.ConnectorTag.{DocumentConnectorTag, RelationalConnectorTag}
import util._

class SelfRelationFilterBugSpec extends FlatSpec with Matchers with ApiSpecBase {
  override def runOnlyForCapabilities = Set(JoinRelationLinksCapability)

  val project = ProjectDsl.fromString {
    connectorTag match {
      case _: RelationalConnectorTag =>
        """model Category {
          |  id       String    @id @default(cuid())
          |  name     String
          |  parent   Category? @relation(name: "C", references: [id])
          |  opposite Category? @relation(name: "C")
          |}"""

      case _: DocumentConnectorTag =>
        """model Category {
          |  id       String    @id @default(cuid())
          |  name     String
          |  parent   Category? @relation(name: "C", references: [id])
          |  opposite Category? @relation(name: "C")
          |}"""
    }
  }

  database.setup(project)
  val id = server
    .query("""mutation{createCategory(data:{name: "Sub", parent: {create:{ name: "Root"}} }){parent{id}}}""", project)
    .pathAsString("data.createCategory.parent.id")

  "Getting all categories" should "succeed" in {
    val allCategories =
      s"""{
         |  allCategories: categories {
         |    name
         |    parent {
         |      name
         |    }
         |  }
         |}"""

    val res1 = server.query(allCategories, project).toString
    res1 should be("""{"data":{"allCategories":[{"name":"Sub","parent":{"name":"Root"}},{"name":"Root","parent":null}]}}""")
  }

  "Getting root categories categories" should "succeed" in {

    val rootCategories =
      s"""{
         |  allRootCategories: categories(where: { parent: null }) {
         |    name
         |    parent {
         |      name
         |    }
         |  }
         |}"""

    val res2 = server.query(rootCategories, project).toString
    res2 should be("""{"data":{"allRootCategories":[{"name":"Root","parent":null}]}}""")
  }

  "Getting subcategories with not" should "succeed" taggedAs (IgnoreMongo) in {

    val subCategories = s"""{
                               |  allSubCategories: categories(
                               |    where: {NOT:[{parent: null}] }
                               |  ) {
                               |    name
                               |    parent {
                               |      name
                               |    }
                               |  }
                               |}"""

    val res3 = server.query(subCategories, project).toString
    res3 should be("""{"data":{"allSubCategories":[{"name":"Sub","parent":{"name":"Root"}}]}}""")
  }

  "Getting subcategories with value" should "succeed" in {

    val subCategories2 = s"""{
                           |  allSubCategories2: categories(
                           |    where: {parent: {name: "Root"} }
                           |  ) {
                           |    name
                           |    parent {
                           |      name
                           |    }
                           |  }
                           |}"""

    val res4 = server.query(subCategories2, project).toString
    res4 should be("""{"data":{"allSubCategories2":[{"name":"Sub","parent":{"name":"Root"}}]}}""")

  }

}
