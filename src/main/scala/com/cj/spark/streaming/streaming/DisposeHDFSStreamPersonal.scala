package com.cj.spark.streaming.streaming

import com.alibaba.fastjson.JSON.parseObject
import com.cj.spark.streaming.models._
import com.cj.spark.streaming.streaming.StartStreaming.log
import com.cj.util.ConfigerHelper
import com.cj.util.DBHelper.getProp
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.language.postfixOps

object DisposeHDFSStreamPersonal {
  private[this] val hdfs_data_path = ConfigerHelper.getProperty("hdfs.data")
  private[this] val jdbcUrl = ConfigerHelper.getProperty("jdbc.url")
  private[this] val jdbcUrlTest = ConfigerHelper.getProperty("jdbc.url.test")
  private[this] val isTest = ConfigerHelper.getProperty("isTest").toBoolean
  private[this] val prop = getProp(isTest)
  private[this] val tech_before_class_investig = Set(202, 203)
  private[this] val tech_exam_reply = Set(601, 602, 603, 604, 605, 606, 607)
  private[this] val objectives_question = Set(301, 302, 303, 304)
  private[this] val tech_question = Set(301, 302, 303, 304, 305)
  private[this] val choice_question = Set(301, 302, 304)
  private[this] val name = Set("tb_tech_interact", "tb_tech_group_interact", "tb_tech_investigation", "tb_tech_practise", "tb_tech_before_class_investig")
  private[this] val types = Set(102, 103, 111, 402, 411, 201, 202, 203, 205, 301, 302, 303, 304, 305, 311)
  private[this] val reply_name = Set("tb_tech_interact_reply", "tb_tech_group_interact_reply", "tb_tech_investigation_reply", "tb_tech_before_class_investig_reply", "tb_tech_practise_reply")

  def createStreamingContext(checkpointDirectory:String,appName: String): StreamingContext = {
    val spark: SparkSession = SparkSession.builder()
      .appName(appName)
//      .master("local[2]")
      //      .config("spark.default.parallelism", "12")
      .config("spark.shuffle.consolidateFiles", true)
      .config("spark.streaming.backpressure.enabled", true)
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))


    val ds = ssc.textFileStream(hdfs_data_path)

    var url = jdbcUrl
    if (isTest) url = jdbcUrlTest
    dispose(ds, sc, spark, appName, url)
    ssc
  }

  def dispose(DStream: DStream[String],
              @transient sct: SparkContext,
              @transient spark: SparkSession,
              appName: String,
              url: String): Unit = {
    DStream.foreachRDD(foreachFunc = rdd => {
      if (!rdd.isEmpty()) {

          val json_name_dataARR = rdd.map(x => parseObject(x)).map(x => x.getJSONArray("RecordList").toArray().map(x => parseObject(x.toString)))
            .flatMap(x =>
              x.map(s => (s.getString("fStr_TableName"), s.getJSONArray("RecordList").toArray()))
            ).cache()

        json_name_dataARR.count()

        val stuLogin = json_name_dataARR.filter(_._1 == "tb_tech_student_login_record").flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_StudentID"),
          json.getString("fStr_StudentName"),
          json.getString("fStr_CourseID")
        )).distinct().cache()
        val stuLoginMap = stuLogin.map(line => (line._3, 1)).reduceByKey(_ + _).collectAsMap()

        val student_info = stuLogin.map(line => ((line._3, line._1), line._2))

        val courseID = stuLogin.map(_._3).distinct().collect()
        val stuLoginIDMap = stuLogin.map(line => (line._3, (line._1, line._2))).groupByKey().mapValues(iter => {
          iter.toMap
        }).collectAsMap()

        val interact_issue = json_name_dataARR.filter(x => name.contains(x._1))
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType")
        )).filter(x => types.contains(x._4)).distinct()

        val personal_development = json_name_dataARR.filter(_._1 == "tb_tech_personal_development")
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getString("fStr_StudentID"),
          json.getString("fStr_SubjectName"),
          json.getString("fStr_Json"),
          json.getString("fDtt_CreateTime"),
          json.getString("fDtt_ModifyTime")
        ))

        //          val classExamCount = json_name_dataARR.filter(_._1 == "tb_tech_exam")
        //            .flatMap(x => x._2)
        //            .map(line => parseObject(line.toString))
        //            .map(json => (
        //              json.getString("fStr_CourseID"),
        //              json.getString("fStr_ExamID")))
        //            .distinct().map(line => (line._1, 1)).reduceByKey(_ + _).collectAsMap()
        //每个课堂双向互动次数

        var interact_count = interact_issue.map(line => (line._1, 1)).reduceByKey(_ + _).collectAsMap()


        //每种双向互动应影响人数
        //        val interactStuTotal = interact_issue.map(line => ((line._1, line._4), stuLoginMap(line._1))).distinct()
        //        interactStuTotal.foreach(println(_))

        /**
          * 互动响应
          * 不能判断考试内容是考试讲解还是考试过程
          */

        //          val examReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_exam_reply")
        //            .flatMap(x => x._2)
        //            .map(line => parseObject(line.toString)).map(json => (
        //            json.getString("fStr_CourseID"),
        //            json.getIntValue("fInt_ProcessNo"),
        //            json.getIntValue("fInt_InteractNo"),
        //            json.getIntValue("fInt_InteractType"),
        //            json.getString("fStr_StudentID")
        //          )).filter(x => tech_exam_reply.contains(x._4))
        //            .map(line => (line._1, line._2, line._3, 600, line._5))

        val evaluateReplyRdd = json_name_dataARR.filter(_._1 == "tb_tech_evaluate").flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          111,
          json.getString("fStr_StudentID")
        ))

        val contributionReplyRdd = json_name_dataARR.filter(x => x._1 == "tb_tech_group_contribution" || x._1 == "tb_tech_group_evaluate").flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          411,
          json.getString("fStr_StudentID")
        ))
        /**
          * type_stu_times 互动类型编号 学生ID 此学生次互动次数
          */
        val type_stu_times = json_name_dataARR.filter(x => reply_name.contains(x._1)).flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID")
        )).filter(x => types.contains(x._4))
          .union(evaluateReplyRdd)
          .union(contributionReplyRdd)
          //            .union(examReplyRdd)
          .distinct()
          .map(line => ((line._1, line._4, line._5), 1))
          .reduceByKey(_ + _).cache()
        //每个课堂每个互动人数
        val response = type_stu_times.
          map(line => ((line._1._1, line._1._2), 1))
          .reduceByKey(_ + _).cache()
        val noResponseManCount = response.map(line => (line._1, stuLoginMap.getOrElse(line._1._1, 0) - line._2)).filter(_._2 > 0).reduceByKey(_ + _)
        /**
          * allInteractManCount  所有互动人数SUM
          */

        val allInteractManCount = response.map(line => (line._1._1, line._2.toDouble)).reduceByKey(_ + _)
        //        println(s"response ${response.partitions.size}")
        /**
          * 关注度 所有互动人数/（签到人数*发布的互动数）
          * 课堂 、关注度
          */
        val focusRate = allInteractManCount.map(line => (line._1, (line._2.toDouble / (interact_count.getOrElse(line._1, 0) * stuLoginMap.getOrElse(line._1, 0)) * 100)
          .formatted("%.2f").toDouble)).collectAsMap()


        //最多未响应互动  （课堂（互动，人数））
        val noResponseManCountMsg = noResponseManCount.map(line => (line._1._1, (line._1._2, line._2))).groupByKey().mapValues(f => {
          f.toList.maxBy(_._2)
        }).collectAsMap()

        /**
          * practiseQuestionRdd 练习题发布
          */
        val practiseQuestionRdd = json_name_dataARR.filter(_._1 == "tb_tech_question")
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_QuestionType"),
          json.getString("fDtt_CreateTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_RightAnswerText")
        )).filter(x => tech_question.contains(x._4))
          .distinct().cache()

        /**
          * 练习题问题及答案
          */

        val questionAnswer = practiseQuestionRdd.map(line => ((line._1, line._2, line._3, line._6, line._7), (line._4, line._8, line._5)))
          .groupByKey().mapValues(iter => {
          iter.toList.maxBy(_._2)
        }).map(line => (line._1, (line._2._1, line._2._2)))


        /**
          * practiseAnswerRdd 练习题回答
          */
        val practiseAnswerPadRdd = json_name_dataARR.filter(_._1 == "tb_tech_practise_reply")
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fDtt_AnswerTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_AnswerText"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fStr_StudentID")
        )).filter(x => tech_question.contains(x._4) && x._9 == 1)
          .distinct()

        /**
          * practiseAnswerRdd 练习题回答
          * 卷测拆分为301~305
          */
        val practiseAnswerRdd1 = json_name_dataARR.filter(_._1 == "tb_tech_practise_reply")
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fDtt_AnswerTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_AnswerText"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fStr_StudentID")
        )).filter(x => x._4 == 311 && x._9 == 1)
          .distinct()
          .map(line => ((line._1, line._2, line._3, line._6, line._7), (line._5, line._8, line._9, line._10)))
          .join(questionAnswer)
          .map(line => (line._1._1, line._1._2, line._1._3, line._2._2._1, line._2._1._1, line._1._4, line._1._5,
            line._2._1._2, line._2._1._3,
            line._2._1._4))
        val practiseAnswerRdd = practiseAnswerRdd1.union(practiseAnswerPadRdd)


        /**
          * 每个学生每道题最后所答
          */

        val practiseLastAnswer = practiseAnswerRdd.map(line => ((line._1, line._2, line._3, line._4, line._6, line._7, line._10), (line._5, line._8)))
          .groupByKey().mapValues(iter => {
          iter.toList.maxBy(_._2)
        }).map(line => ((line._1._1, line._1._2, line._1._3, line._1._5, line._1._6, line._1._4), (line._1._7, line._2._2)))
          .cache()


        /**
          * 客观题问题及答案
          */
        val objectivesQuestionAnswer = questionAnswer.filter(
          x => objectives_question.contains(x._2._1))
          .map(line => (
            (line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._2._1), line._2._2)
          )

        /**
          * 主观题问题及答案
          */
        val subjectiveQuestionAnswer = questionAnswer.filter(_._2._1 == 305).map(line => (
          (line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._2._1), line._2._2)
        ).cache()

        /**
          * 习题正确答案
          */
        val questionAnswerMap = questionAnswer.map(line => (
          (line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._2._1), line._2._2)
        ).collectAsMap()


        /**
          * 客观题正确率
          */
        val objectivesLastAnswer = practiseLastAnswer
          .filter(x => objectives_question.contains(x._1._6))
          .filter(x => questionAnswerMap.keySet.contains(x._1)).cache()
        val objectivesRight = objectivesLastAnswer.filter(x => show(questionAnswerMap.get(x._1)) == x._2._2)
        //每个课堂所有人正确的客观题总数
        val objectivesRightNum = objectivesRight.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()

        //客观题发布数
        val objectivesQuestionAnswerNum = objectivesQuestionAnswer.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()


        val objectivesRightRatio = objectivesRightNum.map(line => (line._1,
          (line._2.toDouble / (objectivesQuestionAnswerNum.getOrElse(line._1, 0) * stuLoginMap.getOrElse(line._1, 0)) * 100)
            .formatted("%.2f").toDouble))

        //          println(s"practiseLastAnswer:${practiseLastAnswer.partitions.size}")
        /**
          * 主观题完成率
          */
        //所有人主观题回答
        val subjectiveLastAnswer = practiseLastAnswer.filter(_._1._6 == 305).cache()
        val subjectiveAnswer = subjectiveLastAnswer.filter(x => questionAnswerMap.keySet.contains(x._1)).map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()


        //主观题发布数
        val subjectiveQuestionAnswerNum = subjectiveQuestionAnswer.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()

        val subjectiveOverRatio = subjectiveAnswer.map(line => (line._1, (line._2.toDouble / (subjectiveQuestionAnswerNum.getOrElse(line._1, 0) * stuLoginMap.getOrElse(line._1, 0)) * 100).formatted("%.2f").toDouble))


        /**
          * 主观+客观   综合 正确率
          * （客观题正确数*10+主观题获得分）/客观题发布数*签到人数*10+有效[批改并且有分主动提交]
          */
        val practiseCorrectRdd = json_name_dataARR.filter(_._1 == "tb_tech_practise_correct")
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fDtt_CreateTime"),
          json.getIntValue("fInt_QuestionNo"),
          json.getIntValue("fInt_SubQuesNo"),
          json.getString("fStr_CorrectID"),
          json.getDoubleValue("fFlt_Score")
        )).filter(x => x._4 == 1 && x._9 != -1)
          .distinct().cache()
        val practiseCorrectLast = practiseCorrectRdd.map(line => ((line._1, line._2, line._3, line._6, line._7, line._8), (line._9, line._5)))
          .groupByKey().mapValues(iter => {
          iter.toList.maxBy(_._2)
        }).map(line => (line._1, line._2._1))
        val practiseCorrectLastNum = practiseCorrectLast.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()

        val practiseCorrectScore = practiseCorrectLast.map(line => (line._1._1, line._2)).reduceByKey(_ + _).collectAsMap()

        //综合正确率
        val correctRate = objectivesRightNum.map(line => (line._1,
          ((line._2.toDouble * 10 + practiseCorrectScore.getOrElse(line._1, 0.0)) /
            (objectivesQuestionAnswerNum.getOrElse(line._1, 0) * 10 * stuLoginMap.getOrElse(line._1, 0) + practiseCorrectLastNum.getOrElse(line._1, 0) * 10) * 100).formatted("%.2f").toDouble))


        /**
          * 预习率&复习率
          */
        val investigationClassRdd = json_name_dataARR.filter(_._1 == "tb_tech_before_class_investig_reply")
          .flatMap(x => x._2)
          .map(line => parseObject(line.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_InteractType"),
          json.getString("fStr_StudentID"),
          json.getString("fStr_AnswerText"),
          json.getString("fDtt_AnswerTime")
        )).filter(x => tech_before_class_investig.contains(x._4) && x._6.nonEmpty)
          .distinct().cache()

        val investigationClassLast = investigationClassRdd.map(line => ((line._1, line._2, line._3, line._4, line._5), (line._6, line._7)))
          .groupByKey().mapValues(iter => {
          iter.toList.maxBy(_._2)
        })
        //        println(s"investigationClassLast : ${investigationClassLast.partitions.size}")
        val beforeClass = investigationClassLast.filter(_._1._4 == 202).cache()

        //预习人数
        val beforeClassInvestigationManCount_yes = beforeClass.filter(_._2._1 == "预习了").map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()


        //预习调查人数
        val beforeClassInvestigationManCount = beforeClass.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()

        //预习率
        val beforeClassRatio = beforeClassInvestigationManCount_yes.map(line => (line._1, (line._2.toDouble / beforeClassInvestigationManCount.getOrElse(line._1, 0) * 100)
          .formatted("%.2f").toDouble))

        //复习数据
        val afterClass = investigationClassLast.filter(_._1._4 == 203).cache()
        //复习人数
        val afterClassInvestigationManCount_yes = afterClass.filter(_._2._1 == "复习了").map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()


        //复习调查人数
        val afterClassInvestigationManCount = afterClass.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()

        //复习率
        val afterClassRatio = afterClassInvestigationManCount_yes.map(line => (line._1, (line._2.toDouble / afterClassInvestigationManCount.getOrElse(line._1, 0) * 100)
          .formatted("%.2f").toDouble))


        /**
          * 个人关注度
          */
        val everyFocus = type_stu_times.map(line => ((line._1._1, line._1._3), line._2)).reduceByKey(_ + _).map(data => (data._1,
          (data._2.toDouble / interact_count.getOrElse(data._1._1, 0) * 100).formatted("%.2f").toDouble))

        /**
          * 个人客观题回答正确率
          */
        //每个学会回答客观题正确题数
        val everyObjectivesRight = objectivesRight.map(data =>
          ((data._1._1, data._2._1), 1)
        ).reduceByKey(_ + _)
        //        println(s"everyObjectivesRight: ${everyObjectivesRight.partitions.size}")
        //个人客观题回答正确率
        val everyBodyObjectivesRightRatio = everyObjectivesRight.map(line =>
          (line._1, (line._2.toDouble / objectivesQuestionAnswerNum.getOrElse(line._1._1, 0) * 100).formatted("%.2f").toDouble))

        //        println(s"everyBodyObjectivesRightRatio ${everyBodyObjectivesRightRatio.partitions.size}")
        /**
          * 客观题每道正确率
          *
          */
        val everyObjectivesRightCount = objectivesQuestionAnswer
          .join(objectivesRight.map(line => (line._1, 1)).reduceByKey(_ + _))
          .map(line => (line._1, line._2._2))
        //客观题每道正确率
        val everyObjectivesRightRatio = everyObjectivesRightCount.mapPartitions(iter => {
          iter.map(line =>
            (line._1, (line._2.toDouble / stuLoginMap.getOrElse(line._1._1, 0) * 100).formatted("%.2f").toDouble))

        })

        /**
          * 主观题每道题完成率
          */

        val everySubjectiveOverCount = subjectiveQuestionAnswer
          .join(subjectiveLastAnswer.map(data => (data._1, 1)).reduceByKey(_ + _))
          .map(data => (data._1, data._2._2))
        //每道主观题的完成率
        val everySubjectiveOverRatio = everySubjectiveOverCount.map(data =>
          (data._1, (data._2.toDouble / stuLoginMap.getOrElse(data._1._1, 0) * 100).formatted("%.2f").toDouble))
        /**
          * 选择&判断每道题选择人数，未答人
          */
        val choice = objectivesLastAnswer.filter(x => choice_question.contains(x._1._6)).cache()
        val choiceAnswerNum = choice
          .map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._1._6, data._2._2), data._2._1))
          .groupByKey().map(data => (data._1, data._2.size))
        val everyChoiceAnswerNum = choiceAnswerNum.map(data =>
          ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._1._6), (data._1._7, data._2)))
        //选择&判断每道题的未答人
        val everyQuseionNoAnswer = choice.map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._1._6),
          data._2._1))
          .groupByKey()
          .map(data => (data._1, {
            stuLoginIDMap(data._1._1).keySet -- data._2.toSet
          })).filter(_._2.nonEmpty).cache()

        /**
          * 对应题
          */
        val corresponding = objectivesLastAnswer.filter(x => x._1._6 == 303)
        val correspondingTmp = corresponding.map(data => (data._1, data._2._2))


        //每道对应题的错几人数 题,错几，几人
        val everyCorrespondingMistakeNumRdd = objectivesQuestionAnswer
          .filter(x => x._1._6 == 303).join(correspondingTmp).filter(_._2._2.nonEmpty).map(data => (data._1, {
          val right = data._2._1.replaceAll("\\d+", "").split("")
          val answer = data._2._2.replaceAll("\\d+", "").split("")
          var rightNum = 0
          for (x <- 0 until answer.size - 1) {
            if (right(x) == answer(x)) rightNum += 1
          }
          right.size - rightNum
        })).map(data => ((data._1._1, data._1._2, data._1._3, data._1._4, data._1._5, data._1._6, data._2), 1))
          .reduceByKey(_ + _)


        val overEvaluationClassRdd = json_name_dataARR.filter(_._1 == "tb_tech_overclass")
          .flatMap(x => x._2)
          .map(data => parseObject(data.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getString("fStr_StudentID"),
          json.getDoubleValue("fFlt_TeacherScore"),
          json.getDoubleValue("fFlt_SelfScore"),
          json.getIntValue("fInt_SubmitType"),
          json.getString("fDtt_AnswerTime")
        )).filter(_._5 == 1).distinct().cache()


        val overEvaluationClass = overEvaluationClassRdd.map(line => ((line._1, line._2), line._3, line._4))
        val evaluationStuNum = overEvaluationClass.map(line => (line._1._1, 1)).reduceByKey(_ + _).collectAsMap()
        /**
          * 评价教师均分
          */
        val evaluationForTeacher = overEvaluationClass.map(line => (line._1._1, line._2)).reduceByKey(_ + _).map(line => (line._1,
          line._2 / evaluationStuNum.getOrElse(line._1, 0)
        )).collectAsMap()

        /**
          * 自评均分
          */
        val evaluationForSelf = overEvaluationClass.map(line => (line._1._1, line._3)).reduceByKey(_ + _).map(line => (line._1,
          line._2 / evaluationStuNum(line._1)
        )).collectAsMap()
        /**
          * 自评分
          */
        val selfLast = overEvaluationClass.map(line => (line._1, line._3))

        val perceptionRdd = json_name_dataARR.filter(_._1 == "tb_tech_perception")
          .flatMap(x => x._2)
          .map(data => parseObject(data.toString)).map(json => (
          json.getString("fStr_CourseID"),
          json.getIntValue("fInt_ProcessNo"),
          json.getIntValue("fInt_InteractNo"),
          json.getIntValue("fInt_QuestionNo"),
          json.getString("fStr_StudentID"),
          json.getIntValue("fInt_PerceptionValue"),
          json.getString("fStr_PerceptionName"),
          json.getIntValue("fInt_SubmitType")
        )).distinct().cache()

        /**
          * 标签前三
          */
        val perceptionTagTop = perceptionRdd
          .map(data => (data._1, (data._6, data._7)))
          .map(data => ((data._1, data._2), 1))
          .reduceByKey(_ + _).map(line => (line._1._1, (line._1._2, line._2)))
          .groupByKey().mapValues(iter => {
          iter.toList.sortBy(_._2).takeRight(3)
        })


        val effect = courseID.map(id => {
          Tuple17(
            id,
            interact_count.getOrElse(id, 0),
            stuLoginMap.getOrElse(id, 0),
            noResponseManCountMsg.getOrElse(id, (-1, -1))._1,
            noResponseManCountMsg.getOrElse(id, (-1, -1))._2,
            objectivesQuestionAnswerNum.getOrElse(id, 0),
            objectivesRightRatio.getOrElse(id, 0.0),
            subjectiveOverRatio.getOrElse(id, 0.0),
            subjectiveQuestionAnswerNum.getOrElse(id, 0),
            focusRate.getOrElse(id, 0.0),
            correctRate.getOrElse(id, 0.0),
            evaluationForTeacher.getOrElse(id, 0.0),
            evaluationStuNum.getOrElse(id, 0),
            evaluationForSelf.getOrElse(id, 0.0),
            evaluationStuNum.getOrElse(id, 0),
            beforeClassRatio.getOrElse(id, 0.0),
            afterClassRatio.getOrElse(id, 0.0)
          )

        })


        import spark.implicits._

        /**
          * 课堂信息
          *
          */
        sct.parallelize(effect)
          .map(line => tb_teach_msg(line._1, line._2.toInt, line._3.toInt, line._4,
            line._5, line._6.toInt, line._7, line._8, line._9.toInt,
            line._10, line._11, line._12, line._13.toInt,
            line._14, line._15.toInt, line._16, line._17))
          .toDF().write.mode("append").jdbc(url, "tb_teach", prop)

        perceptionTagTop.flatMap(line => line._2.map(data => (line._1, data._1._1, data._1._2, data._2)))
          .map(line => tb_perception(line._1, line._2, line._3, line._4))
          .toDF().write.mode("append").jdbc(url, "tb_perception", prop)

        /**
          * 每道题没有回答的人
          */
        everyQuseionNoAnswer.flatMap(line => line._2.map(data => (line._1._1,
          line._1._2,
          line._1._3,
          line._1._4,
          line._1._5,
          line._1._6,
          data))).map(line => tb_question_no_answer(line._1, line._2, line._3, line._4, line._5, line._6, line._7))
          .toDF().write.mode("append").jdbc(url, "tb_question_no_answer", prop)
        everyChoiceAnswerNum
          .map(line => tb_choice_judge_question(line._1._1, line._1._2, line._1._3, line._1._4, line._1._5, line._1._6, line._2._1, line._2._2)).toDF()
          .write.mode("append").jdbc(url, "tb_choice_judge_question", prop)

        /**
          * 对应题，每道题错的个数的人数，如一个 2人，错两个 5人
          */
        everyCorrespondingMistakeNumRdd.map(line => tb_corresponding_question(line._1._1, line._1._2
          , line._1._3
          , line._1._4
          , line._1._5
          , line._1._6
          , line._1._7
          , line._2)).toDF().write.mode("append").jdbc(url, "tb_corresponding_question", prop)

        /**
          * 每道题的正确率、完成率，主观题时信息为完成率，客观题时为正确率
          */
        everyObjectivesRightRatio.union(everySubjectiveOverRatio).map(line => tb_question_correct(line._1._1, line._1._2
          , line._1._3
          , line._1._4
          , line._1._5
          , line._1._6
          , line._2
        )).toDF().write.mode("append").jdbc(url, "tb_question_correct", prop)

        /**
          * 学生信息 学生ID，个人关注度，客观题正确率，自评分
          *
          */

        val stu_rdd = everyFocus.cogroup(everyBodyObjectivesRightRatio, selfLast, student_info).map(kv => {
          (kv._1,
            if (kv._2._1.nonEmpty) kv._2._1.head else 0.0,
            if (kv._2._2.nonEmpty) kv._2._2.head else 0.0,
            if (kv._2._3.nonEmpty) kv._2._3.head else 0.0,
            if (kv._2._4.nonEmpty) kv._2._4.head else "")
        }).map(line => (line._1._1, line._1._2, line._2, line._3, line._4, line._5))

        stu_rdd.map(line => tb_student(
          line._1,
          line._2,
          line._3,
          line._4,
          line._5,
          line._6)).toDF().write.mode("append").jdbc(url, "tb_student", prop)

        /**
          * 成长记录写入数据库
          */
        personal_development.map(line => tb_personal_development(
          line._1,
          line._2,
          line._3,
          line._4,
          line._5,
          line._6)).toDF().write.mode("append").jdbc(url, "tb_personal_development", prop)

        unPersistUnUse(Set(), sct)

    }


  })
}


def show(x: Option[String]): String = x match {
  case Some(s) => s
  case None => "?"
}


  /**
    * 差值 econd - first
    *
    * @return
    */
  def diffValue(first: Iterable[Int], second: Iterable[Int]): Int = {
  if (first.nonEmpty) {
  if (second.nonEmpty) {

  second.head - first.head
} else {
  -1
}
} else {
  second.head
}

}


  /**
    * 释放RDD缓存
    *
    * @param rddString 不需要释放的RDD
    * @param sc        SparkContext
    */
  def unPersistUnUse(rddString: Set[String], sc: SparkContext): Unit = {
  val persistRDD = sc.getPersistentRDDs
  persistRDD.foreach(f = tuple => {
  if (!rddString.contains(tuple._2.toString())) {
  log.info(s"释放缓存RDD\t[${tuple._2.toString()}]")
  tuple._2.unpersist()
}
})
}


}

