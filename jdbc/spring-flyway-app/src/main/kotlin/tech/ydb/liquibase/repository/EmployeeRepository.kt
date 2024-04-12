package tech.ydb.liquibase.repository

import org.springframework.data.repository.CrudRepository
import tech.ydb.liquibase.model.Employee

interface EmployeeRepository : CrudRepository<Employee, Long>

fun EmployeeRepository.findByIdOrNull(id: Long): Employee? = this.findById(id).orElse(null)
