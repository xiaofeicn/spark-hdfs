package com.cj.spark.streaming.models

case class tb_teach(v_tid:String,
                  ui_interaction_num:Int,
                  ui_student_sign:Int,
                  ui_no_response:Int,
                  ui_no_response_count:Int,
                  ui_objective_question_num:Int,
                  ui_objective_question_right_ratio:Double,
                  ui_subjective_question_over_ratio:Double,
                  ui_subjective_question_num:Int,
                  focusRate:Double,
                  correctRate:Double,
                  teacher_mark_avg:Double,
                  teacher_mark_stu_num:Int,
                  oneself_mark_avg:Double,
                  oneself_mark_num:Int,
                  before_class_ratio:Double,
                  after_class_ratio:Double,
                  perception_tag_three:String
                 )
case class tb_student(v_tid:String,
                      ui_student_id:String,
                      stu_focusRate:Double,
                      objective_correctRate:Double,
                      self_mark:Double,
                      v_student_name:String)

case class tb_question_msg(v_tid:String,
                           ui_process_no:Int,
                           ui_interaction_no:Int,
                           ui_question_id:Int,
                           ui_subquestion_no:Int,
                           ui_question_type:Int,
                           finishRate_correctRate:Double,
                           every_choice_Num:String,
                           no_answer_stu:String,
                           everyCorresponding_mistake_Num:String)

case class tb_teach_msg(v_tid:String,
                        ui_interaction_num:Int,
                        ui_student_sign:Int,
                        ui_no_response:Int,
                        ui_no_response_count:Int,
                        ui_objective_question_num:Int,
                        ui_objective_question_right_ratio:Double,
                        ui_subjective_question_over_ratio:Double,
                        ui_subjective_question_num:Int,
                        focusRate:Double,
                        correctRate:Double,
                        class_mark_avg:Double,
                        class_mark_stu_num:Int,
                        oneself_mark_avg:Double,
                        oneself_mark_num:Int,
                        before_class_ratio:Double,
                        after_class_ratio:Double
                       )

case class tb_perception(v_tid:String,
                         ui_perception_id:Int,
                          v_perception_name:String,
                          ui_perception_times:Int)

case class tb_question_no_answer(
          v_tid:String,
          ui_process_no:Int,
          ui_interaction_no:Int,
          ui_question_id:Int,
          ui_subquestion_no:Int,
          ui_question_type:Int,
          ui_student_id:String

)

case class tb_personal_development(
                                  v_tid:String,
                                  ui_student_id:String,
                                  v_subject_name:String,
                                  v_json:String,
                                  dt_create_time:String,
                                  dt_modify_time:String

                                )


case class tb_choice_judge_question(v_tid:String,
ui_process_no:Int,
ui_interaction_no:Int,
ui_question_id:Int,
ui_subquestion_no:Int,
ui_question_type:Int,
selected:String,
selected_times:Int)

case class tb_corresponding_question(v_tid:String,
                                    ui_process_no:Int,
                                    ui_interaction_no:Int,
                                    ui_question_id:Int,
                                    ui_subquestion_no:Int,
                                    ui_question_type:Int,
                                     ui_mistake_num:Int,
                                     ui_mistake_sut_num:Int)
case class tb_question_correct(v_tid:String,
                                     ui_process_no:Int,
                                     ui_interaction_no:Int,
                                     ui_question_id:Int,
                                     ui_subquestion_no:Int,
                                     ui_question_type:Int,
                               finishRate_correctRate:Double)

class Tables{

}