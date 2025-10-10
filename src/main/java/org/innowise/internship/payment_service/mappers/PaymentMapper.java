package org.innowise.internship.payment_service.mappers;

import org.mapstruct.BeanMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

import org.innowise.internship.payment_service.dto.CreatePaymentDTO;
import org.innowise.internship.payment_service.dto.ResponsePaymentDTO;
import org.innowise.internship.payment_service.dto.UpdatePaymentDTO;
import org.innowise.internship.payment_service.entities.Payment;
import org.mapstruct.NullValuePropertyMappingStrategy;

@Mapper(componentModel = "spring")
public interface PaymentMapper {

    @Mapping(target = "id", ignore = true)
    Payment createPaymentDTOTOPayment(CreatePaymentDTO createPaymentDTO);

    @BeanMapping(nullValuePropertyMappingStrategy = NullValuePropertyMappingStrategy.IGNORE)
    void updatePaymentFromPaymentUpdateDTO(UpdatePaymentDTO updatePaymentDTO, @MappingTarget Payment payment);

    ResponsePaymentDTO PaymentToResponseDTO(Payment payment);}
